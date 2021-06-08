all: src/pagerank.c
	gcc src/pagerank.c -o pagerank -pthread -Wall 
