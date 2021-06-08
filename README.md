 -=-=-=- Compile -=-=-=-
    > gcc pagerank.c -o pagerank –pthread
    
 -=-=-=- Run -=-=-=-
    > ./pagerank <filename> -t <number_of_threads>
    
    filename should be a text file where each line contains two integers
    seperated with TAB that represent the node number. Lines starting 
    with (#) will be ignored. Sample test files are included.
    
    Uppon completion the program will generate a text file containing 
    the nodes of the graph and their pagerank.
