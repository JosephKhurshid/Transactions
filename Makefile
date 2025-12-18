all: transactions

transactions: transactions.cpp
	g++ -std=c++20 -O3 -pthread -fgnu-tm -Wall -Wextra -o transactions transactions.cpp -litm

clean:
	rm -f transactions
