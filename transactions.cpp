#include <iostream>     
#include <vector>       
#include <thread>       
#include <mutex>        
#include <atomic>
#include <random>
#include <chrono>
#include <algorithm>//sort
#include <cstring>      
#include <iomanip>//fixed and setprecision for format
#include <string>
using namespace std;

//TODO: consistently check if from account and to account are the same. just doing it inconsitently. f it, just enforce in testing
//try some bigger numbers. 
int NUM_ACCOUNTS = 1000;      
int INITIAL_BALANCE = 1000;   
atomic<bool> running{true};

class Bank {
public:
    virtual ~Bank() = default;    
    virtual bool transfer(int from_account, int to_account, int transfer_ammount) = 0;    
    virtual long long total_balance_accross_all_accounts() = 0;
    virtual string name() = 0;
};

class SingleGlobalLockBank : public Bank {
    vector<int> accounts; 
    mutex single_global_lock;

public:
    SingleGlobalLockBank() : accounts(NUM_ACCOUNTS, INITIAL_BALANCE) {}

    bool transfer(int from_account, int to_account, int transfer_ammount) override {
        single_global_lock.lock();
        if (accounts[from_account] >= transfer_ammount) {
            accounts[from_account] = accounts[from_account] - transfer_ammount;
            accounts[to_account] = accounts[to_account] + transfer_ammount;
            single_global_lock.unlock();
            return true;
        }
        single_global_lock.unlock();
        return false;
    }

    long long total_balance_accross_all_accounts() override {
        single_global_lock.lock();//still need to lock, money might be moved in transfer without it
        long long total = 0;
        for (size_t i = 0; i < accounts.size(); ++i) {
            total = total + accounts[i];          
        }        
        single_global_lock.unlock();
        return total;
    }

    string name() override {
        return "SingleGlobalLockBank";
    }
};


class TwoPhaseAccountClass {
public:
    int balance;
    mutex mtx;  
    TwoPhaseAccountClass() : balance(0) {}
};

class TwoPhaseLockingBank : public Bank {
    vector<TwoPhaseAccountClass> accounts;

public:
    TwoPhaseLockingBank() : accounts(NUM_ACCOUNTS) {
        for (size_t i = 0; i < accounts.size(); ++i) {
            accounts[i].balance = INITIAL_BALANCE;
        }    
    }

    bool transfer(int from_account, int to_account, int transfer_ammount) override {
        int smallest_account_index;
        int greatest_account_index;
        
        if (from_account == to_account){ 
            return true;
        }

        if (from_account < to_account){
            smallest_account_index = from_account;
            greatest_account_index = to_account;
        } else{
            smallest_account_index = to_account;
            greatest_account_index = from_account;
        }

        accounts[smallest_account_index].mtx.lock();
        accounts[greatest_account_index].mtx.lock();

        if (accounts[from_account].balance >= transfer_ammount) {
            accounts[from_account].balance = accounts[from_account].balance  - transfer_ammount;
            accounts[to_account].balance = accounts[to_account].balance + transfer_ammount;
            accounts[greatest_account_index].mtx.unlock();
            accounts[smallest_account_index].mtx.unlock();
            return true;
        }
        
        accounts[greatest_account_index].mtx.unlock();
        accounts[smallest_account_index].mtx.unlock();
        return false;
    }

    long long total_balance_accross_all_accounts() override {
        long long total = 0;

        for (int i = 0; i < NUM_ACCOUNTS; ++i) {
            accounts[i].mtx.lock(); 
        }

        for (int i = 0; i < NUM_ACCOUNTS; ++i) {
            total = total + accounts[i].balance;
        }

        for (int i = 0; i < NUM_ACCOUNTS; ++i) {
            accounts[i].mtx.unlock(); 
        }
        
        return total;
    }

    string name() override {
        return "TwoPhaseLockingBank";
    }
};


class SoftwareTransactionalMemoryBank : public Bank {
    vector<int> accounts;

public:
    SoftwareTransactionalMemoryBank() : accounts(NUM_ACCOUNTS, INITIAL_BALANCE) {}

    bool transfer(int from_account, int to_account, int transfer_ammount) override {
        bool transfer_completed = false;
        /*
        NOTES: on what the is under the hood  __transaction_atomic. 
        --- It is doing the changes in a data structure of some sort before making the changes permenant.So in this stage no other thread is seieng the changes.
        --- But it is also deticing conflicts with other threads doing the same thing. If it detects a conflict it just undoes the changes and restarts the block.
        --- If no conflict, it succeeds. But if there is conflict then it undoes and restarts
        */
        __transaction_atomic {
            if (accounts[from_account] >= transfer_ammount) {
                accounts[from_account] = accounts[from_account] - transfer_ammount;
                accounts[to_account] = accounts[to_account] + transfer_ammount;
                transfer_completed = true;
            }
        }
        return transfer_completed;
    }

    long long total_balance_accross_all_accounts() override {
        long long total = 0;
        __transaction_atomic {
            for (size_t i = 0; i < accounts.size(); ++i) {
                total = total + accounts[i];          
            }
        }
        return total;
    }

    string name() override {
        return "SoftwareTransactionalMemoryBank";
    }
};

//I am making the last bit the lock as explained in the paper. 0 is unlocked, 1 is locked
struct Account {
    int balance;
    atomic<uint64_t> version{0};//last modified, considering switch to uint64_t though don't think I will hit the limit. Will ahve to change others too if I do so. decided to do to bit operations
};

class TL2Bank : public Bank {
    vector<Account> accounts;
    atomic<uint64_t> global_version{0};//global snapshot or clock might be better given the paper

    //for single transactions
    class Transaction {
        TL2Bank& bank;
        uint64_t current_version;

        struct R_Log {
            int account_index; 
            uint64_t version; 
        };
        struct W_Log {
            int account_index; 
            int value; 
        };

        vector<R_Log> read_log;
        vector<W_Log> write_log;

    public:
        Transaction(TL2Bank& b) : bank(b) {}

        void initial() {
            read_log = {};
            write_log = {};
            current_version = bank.global_version.load(memory_order_acquire);
        }

        int load(int index) {
            uint64_t version_one;
            uint64_t version_two;
            int value;
            Account& individual_account = bank.accounts[index];
            
            for (size_t i = 0; i < write_log.size(); ++i) {//checking to see if modified already         
                if (write_log[i].account_index == index) {
                    return write_log[i].value;
                }
            }
            
            while (true) {//heavy lifting of optomistic read
                version_one = individual_account.version.load(memory_order_acquire);
                value = individual_account.balance;
                atomic_thread_fence(memory_order_acquire);//not entirely sure this sovled the issue from earlier. unclear whether necessary b/c given that the executions are so close to each other, wouldn't it be likely to unnecessary.
                version_two = individual_account.version.load(memory_order_acquire);//double checked mentioned in class
                //if it was locked (odd) or versions not equal than no break, redo
                if ((version_one % 2 == 0) && (version_one == version_two)) {
                    break; 
                }

                this_thread::yield();
            }

            //turns out, can absolutely mess up by getting ahead of current_version. dividing because the multiplication of 2 does make it greater (was the probelm earlier).
            if ((version_one / 2) > current_version){ 
                throw runtime_error("Version One ahead of Current Version");
            }

            read_log.push_back({index, version_one});//logging it
            return value;
        }

        void store(int index, int value) {//honestly, consider deleting for simplification
            write_log.push_back({index, value});//saving the write without committing, problem if duplication 
        }

        static bool compare_logs_account_indexes(const W_Log& first_log, const W_Log& second_log) {
            return first_log.account_index < second_log.account_index;
        }
        
        //should write helper functions for all the loops
        bool attempt_to_save() {
            bool locked_by_me = false;
            if (write_log.empty()) return true;
            //sorting by account index IOT avoid deadlock,   
            sort(write_log.begin(), write_log.end(), compare_logs_account_indexes);
            //locks while writes
            for (size_t i = 0; i < write_log.size(); ++i) {
                //wanted it earlier so no repeats of initilizations but came to some issues with other for loops if not new initalization each time.
                uint64_t version = bank.accounts[write_log[i].account_index].version.load(memory_order_relaxed);
                
                if ((version % 2 != 0) || !bank.accounts[write_log[i].account_index].version.compare_exchange_strong(version, version | 1)) {//locking. honestly so much into this and I should have just added and subtracted 1 instead of the bit operations.
                    unlock_locks_before_current_lock(write_log[i].account_index); //Undoing locks before this one
                    return false;
                }
            }

            //need to check the reads
            for (size_t i = 0; i < read_log.size(); ++i) {                
                uint64_t version = bank.accounts[read_log[i].account_index].version.load(memory_order_acquire);

                //don't want to falsely quit (quit if we are holding the locks). Need to rethink this, hoping for a simpler way.
                locked_by_me = false;                
                for (size_t j = 0; j < write_log.size(); ++j) {
                    if (write_log[j].account_index == read_log[i].account_index) {
                        locked_by_me = true;
                        break;
                    }
                }

                //chcking if someone else has the lock or if data is diff 
                if ((!locked_by_me && (version % 2 != 0)) || (version / 2 ) > current_version) {
                    unlock_locks_before_current_lock(-1);
                    return false;
                }
            }

            //saving 
            uint64_t new_version = bank.global_version.fetch_add(1) + 1;
            new_version = (new_version << 1); //unlocked given the above
            for (size_t i = 0; i < write_log.size(); ++i) {
                bank.accounts[write_log[i].account_index].balance = write_log[i].value; //updating balance into real bank mem
                bank.accounts[write_log[i].account_index].version.store(new_version, memory_order_release);//updating the version and setting the lock to unlocked at the same time
            }
            return true;
        }

        void unlock_locks_before_current_lock(int current_lock) {
            uint64_t version;
            for (size_t i = 0; i < write_log.size(); ++i) {
                if (write_log[i].account_index == current_lock) break;
                version = bank.accounts[write_log[i].account_index].version.load(memory_order_relaxed);
                bank.accounts[write_log[i].account_index].version.store(version & ~1, memory_order_release);
            }
        }
    };

public:
    TL2Bank() : accounts(NUM_ACCOUNTS) {
        for (size_t i = 0; i < accounts.size(); ++i) {
            accounts[i].balance = INITIAL_BALANCE;
        }    
    }

    bool transfer(int from_account, int to_account, int transfer_ammount) override {
        Transaction individual_transaction(*this);//not the best name but works for now

        while (running) {
            try {
                individual_transaction.initial();
                int balance_from = individual_transaction.load(from_account);
                int balance_to = individual_transaction.load(to_account);
                
                if (balance_from >= transfer_ammount) {//the money exists in the account
                    individual_transaction.store(from_account, balance_from - transfer_ammount);
                    individual_transaction.store(to_account, balance_to + transfer_ammount);
                    if (individual_transaction.attempt_to_save()) return true;
                } else {
                    return false;//account is broke
                }
            } catch (runtime_error& e) {//should prob make this catch all
                //just want to conitnue without the error messing things up
            }

        }
        return false;//for warning since it should not get here.
    }

    long long total_balance_accross_all_accounts() override {//read this wrong in directions, need to modify to just show all of the individual balances
        Transaction individual_transaction(*this);
        while (true) {
            try {
                individual_transaction.initial();
                long long total = 0;
                //To show all, I should make an array here and store all the values, then show all those values in another for loop once the first for loop is done.
                //That way if an issue arises in the .load in the first for loop, it restarts still
                for (int i = 0; i < NUM_ACCOUNTS; ++i) {
                    total = total + individual_transaction.load(i);
                }
                return total;
            } catch (...) { 

            }
        }
    }
    
    string name() override {
        return "TL2Bank";
    }

};

//Testing below
enum Contention { HIGH, LOW };

void worker(Bank* bank, int thread_id, Contention contention, atomic<long long>* transaction_count) {
    mt19937 rng(thread_id + 100);    
    uniform_int_distribution<int> dist_all(0, NUM_ACCOUNTS - 1);
    uniform_int_distribution<int> dist_hot(0, 9);
    uniform_int_distribution<int> dist_percent(0, 99); 
    long long local_count = 0;
    
    while(running) {
        int from_account;
        int to_account;
        bool is_hot = (contention == HIGH) && (dist_percent(rng) < 90);
        
        if (is_hot) {
            from_account = dist_hot(rng);
        } else {
            from_account = dist_all(rng);
        }

        do {
            if (is_hot) {
                to_account = dist_hot(rng);
            } else {
                to_account = dist_all(rng);
            }

        } while (to_account == from_account); 

        bank->transfer(from_account, to_account, 10);
        local_count++;
    }
    
    *transaction_count += local_count;
}

void testing(Bank* bank, int num_threads, int duration_sec, Contention contention) {
    string contention_string;
    running = true;
    atomic<long long> transaction_count{0};
    vector<thread> threads;
    long long initial_total = bank->total_balance_accross_all_accounts();
    long long expected = (long long) NUM_ACCOUNTS * INITIAL_BALANCE;

    if (contention == HIGH) {
        contention_string = "High";
    } else {
        contention_string = "Low";
    }

    cout << "Testing " << bank->name() << " | Threads: " << num_threads << " | Contention: " << contention_string << endl;
    if (initial_total != expected) {
        cerr << "Something went wrong with the initial balance: " << initial_total << " != " << expected << endl;
    }

    auto start = chrono::high_resolution_clock::now();//don't like auto but without the alternative is confusing.

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, bank, i, contention, &transaction_count);
    }

    this_thread::sleep_for(chrono::seconds(duration_sec));
    
    running = false;
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> diff = end - start;

    long long final_total_balance = bank->total_balance_accross_all_accounts();
    if (final_total_balance != initial_total) {
        cerr << "The total balance should be the same as earlier because money transfered to different accounts but all accounts balances were added up. So something went wrong" << final_total_balance << endl;
    } else {
        cout << "Works! No money missing after all the transfers" << endl;
    }

    double throughput = transaction_count / diff.count();
    cout << "Throughput: " << fixed << setprecision(2) << throughput << " transactions per second" << endl; 
    cout << "\n" << endl;
}

void help(char* execut) {
    cout << "Usage: " << execut << " [options]\n" << "Options:\n" << "  -t <threads> Number of threads (default: 4)\n" << "  -d <seconds> Duration of test (default: 2)\n" << " -h Show this help\n";
}

int main(int argc, char** argv) {
    vector<Bank*> banks;
    int num_threads = 4;
    int duration = 2;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            num_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            duration = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-h") == 0) {
            help(argv[0]);
            return 0;
        }
    }

    banks.push_back(new SingleGlobalLockBank());
    banks.push_back(new TwoPhaseLockingBank());
    banks.push_back(new SoftwareTransactionalMemoryBank());
    banks.push_back(new TL2Bank());

    cout << "Testing with " << num_threads << " threads for " << duration << " seconds each.\n" << endl;
    
    for (size_t i = 0; i < banks.size(); ++i) {
        testing(banks[i], num_threads, duration, LOW);
        testing(banks[i], num_threads, duration, HIGH);
    }

    for (size_t i = 0; i < banks.size(); ++i) {
        delete banks[i];
    }
    return 0;
}
