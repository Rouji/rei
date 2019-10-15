all:
	g++ -o rei -O2 rei.cpp -std=c++17 -lmecab -llmdb

debug:
	g++ -o rei -O0 -g rei.cpp -std=c++17 -lmecab -llmdb
