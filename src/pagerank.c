#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define ITTERATIONS 50

// #define DEBUG
#define TIME_OUTPUT

sem_t semaphores[4];
sem_t main_semaphore;
int threadCount;

/* Neighbours of a node */
typedef struct neighbourNode {
    struct node* neighbour;
    struct neighbourNode* next;
} neighbourNode;

/* Node of our Graph */
typedef struct node {
    long nodeNumber;
    double pageRank;
    double pageRankNext;
    double received;

    struct neighbourNode* head;
    int neighbourListSize;

    sem_t node_sem;
} node;

/* list of all nodes of our Graph */
typedef struct gNode {
    node* node;
    struct gNode* next;
} gNode;

struct thread_args {
    gNode* fromNode;
    gNode* toNode;
    int thread_number;
};

size_t listSize;

void printNeighbours(gNode* graphRoot, long nodeNumber);

/* Create a new Node*/
node* newNode(long nodeNuber, double pageRank) {
    node* new = (node*)malloc(sizeof(node));
    new->nodeNumber = nodeNuber;
    new->pageRank = pageRank;
    new->pageRankNext = 0;
    new->received = 0;

    new->head = NULL;
    new->neighbourListSize = 0;

    sem_init(&(new->node_sem), 0, 1);

    return new;
}

/**
 * Given a the number of a node 
 * it returns a pointer to it if found else NULL
 */
gNode* findNode(gNode** root, long nodeNumber) {
    gNode* tmp = *root;
    while (tmp != NULL) {
        if (tmp->node->nodeNumber == nodeNumber)
            return tmp;
        tmp = tmp->next;
    }

    return NULL;
}

/* Add a new node to our Graph */
gNode* addNode(gNode** root, node* _node) {
    gNode* tmp = *root;

    gNode* found = findNode(root, _node->nodeNumber);
    if (found != NULL)
        return found;

    gNode* newgNode = (gNode*)malloc(sizeof(gNode));
    newgNode->node = _node;
    newgNode->next = NULL;

    /*

    newgNode->next = *root;
    *root = newgNode;

    */
    
    if (*root == NULL) {
        *root = newgNode;
    } else {
        while (tmp->next != NULL)
            tmp = tmp->next;

        tmp->next = newgNode;
    }
    
    listSize++;
    return newgNode;
}

neighbourNode* findNeighbour(neighbourNode** head, long nodeNumber) {
    neighbourNode* tmp = *head;
    while (tmp != NULL) {
        if (tmp->neighbour->nodeNumber == nodeNumber)
            return tmp;
        tmp = tmp->next;
    }

    return NULL;
}

/**
 * add neighbour to a specific node
 */
neighbourNode* addNeighbour(neighbourNode** head, node* neighbour) {
#ifdef DEBUG
    printf("adding neighbour node\n");
    printf("neighbour value: %ld\n", neighbour->nodeNumber);
#endif

    /*
    neighbourNode* found = findNeighbour(head, neighbour->nodeNumber);
    if (found != NULL)
        return found;
    */

    neighbourNode* newNeighbour = (neighbourNode*)malloc(sizeof(neighbourNode));
    newNeighbour->neighbour = neighbour;

    newNeighbour->next = *head;
    *head = newNeighbour;

    return newNeighbour;
}

/**
 * Extracts data passed from line pointer 
 * and stores it in nodeNumbers array
 */
void extractData(char* line, long* nodeNumbers) {
    nodeNumbers[0] = atol(strtok(line, "\t"));
    nodeNumbers[1] = atol(strtok(NULL, "\t"));
}

int formGraph(char* filepath, gNode** graphRoot) {
    if (filepath == NULL) {
        printf("Please specify a file with the correct format\n");
        return -1;
    }
    FILE* filePtr = fopen(filepath, "r");

    char* lineBuffer = NULL;
    size_t lineBufferSize = 0;
    ssize_t lineSize;

    long nodeNumbers[2];

    gNode* gNode1 = NULL;
    gNode* gNode2 = NULL;

    lineSize = getline(&lineBuffer, &lineBufferSize, filePtr);
    while (lineSize >= 0) {
#ifdef DEBUG
        printf("Line contains: %s ", lineBuffer);
        printf("line size = %ld\n", lineSize);
#endif
        if (lineBuffer[0] == '#') {
            lineSize = getline(&lineBuffer, &lineBufferSize, filePtr);
            continue;
        }
        extractData(lineBuffer, nodeNumbers);

#ifdef DEBUG
        printf("no1: %ld no2: %ld\n", nodeNumbers[0], nodeNumbers[1]);
#endif

        if (gNode1 != NULL) {
            if (gNode1->node->nodeNumber != nodeNumbers[0]) {
                gNode1 = addNode(graphRoot, newNode(nodeNumbers[0], 1));
            }
        } else {
            gNode1 = addNode(graphRoot, newNode(nodeNumbers[0], 1));
        }

        gNode2 = addNode(graphRoot, newNode(nodeNumbers[1], 1));

        addNeighbour(&(gNode1->node->head), gNode2->node);
        gNode1->node->neighbourListSize++;

        lineSize = getline(&lineBuffer, &lineBufferSize, filePtr);
    }

    fclose(filePtr);

    return 0;
}

void pageRank(gNode** graphRoot) {
    neighbourNode* neighbourHead;
    gNode* tmp;
    double toGive;
    int i;
    for (i = 0; i < ITTERATIONS; i++) {
        tmp = *graphRoot;
        while (tmp != NULL) {
            if (tmp->node->neighbourListSize == 0) {
                tmp = tmp->next;
                continue;
            }

            // calculating amount to be given to neighbours
            toGive = (double)((double)(tmp->node->pageRank * 0.85) / tmp->node->neighbourListSize);
            //calculating amount to be the new pagerank
            tmp->node->pageRankNext = (double)tmp->node->pageRank * 0.15;

            //setting each neighbour received field the right value
            neighbourHead = tmp->node->head;
            while (neighbourHead != NULL) {
                neighbourHead->neighbour->received = neighbourHead->neighbour->received + toGive;
                neighbourHead = neighbourHead->next;
            }
#ifdef DEBUG
            printf("Neighbours of node(%ld): %ld\n", tmp->node->nodeNumber, tmp->node->neighbourListSize);
            printf("\ttoGive: %f\n", toGive);
#endif
            tmp = tmp->next;
        }

        tmp = *graphRoot;

        while (tmp != NULL) {
            tmp->node->pageRank = tmp->node->pageRankNext == 0 ? tmp->node->pageRank + tmp->node->received : tmp->node->pageRankNext + tmp->node->received;
            tmp->node->received = 0;
            tmp->node->pageRankNext = 0;

            tmp = tmp->next;
        }
    }
}

void setPageRank(gNode** graphRoot) {
    gNode* tmp = *graphRoot;

    while (tmp != NULL) {
        tmp->node->pageRank = tmp->node->pageRankNext == 0 ? tmp->node->pageRank + tmp->node->received : tmp->node->pageRankNext + tmp->node->received;
        tmp->node->received = 0;
        tmp->node->pageRankNext = 0;

        tmp = tmp->next;
    }
}

void* par_pageRank(void* arguments) {
    struct thread_args* args = (struct thread_args*)arguments;
    neighbourNode* neighbourHead;
    gNode* tmp;
    double toGive;
    int i;
    for (i = 0; i < ITTERATIONS; i++) {
#ifdef DEBUG
        printf("Thread(%d) started pagerank\n", args->thread_number);
#endif
        sem_wait(&semaphores[args->thread_number]);
        tmp = args->fromNode;
        while (tmp != args->toNode) {
            if (tmp->node->neighbourListSize == 0) {
                tmp = tmp->next;
                continue;
            }

            // calculating amount to be given to neighbours
            toGive = (double)((double)(tmp->node->pageRank * 0.85) / tmp->node->neighbourListSize);
            //calculating amount to be the new pagerank
            tmp->node->pageRankNext = (double)tmp->node->pageRank * 0.15;

            //setting each neighbour received field the right value
            neighbourHead = tmp->node->head;
            while (neighbourHead != NULL) {
                sem_wait(&(neighbourHead->neighbour->node_sem));
                neighbourHead->neighbour->received = neighbourHead->neighbour->received + toGive;
                sem_post(&(neighbourHead->neighbour->node_sem));
                neighbourHead = neighbourHead->next;
            }
#ifdef DEBUG
            printf("Neighbours of node(%ld): %ld\n", tmp->node->nodeNumber, tmp->node->neighbourListSize);
            printf("\ttoGive: %f\n", toGive);
#endif
            tmp = tmp->next;
        }

#ifdef DEBUG
        printf("Thread(%d) finished pagerank\n", args->thread_number);
#endif
        sem_post(&main_semaphore);
    }

    return NULL;
}

void formOutput(gNode* graphRoot) {
    double totalRank = 0;
    struct stat st = {0};
    if (stat("output", &st) == -1) {
        mkdir("output", 0700);
    }
    FILE* outfilePtr = fopen("output/pagerank.csv", "w+");
    fprintf(outfilePtr, "node,\tpagerank\n");
    gNode* tmp = graphRoot;
    while (tmp != NULL) {
        fprintf(outfilePtr, "%ld,\t%lf\n", tmp->node->nodeNumber, tmp->node->pageRank);
        totalRank += tmp->node->pageRank;
        tmp = tmp->next;
    }
#ifdef TIME_OUTPUT
    printf("Total Rank = %lf\n", totalRank);
#endif
}

void printNeighbours(gNode* graphRoot, long nodeNumber) {
    gNode* tmp = graphRoot;
    node* myNode;
    neighbourNode* myNeighbour;
    while (tmp != NULL) {
        if (tmp->node->nodeNumber == nodeNumber) {
            myNode = tmp->node;
            break;
        }
        tmp = tmp->next;
    }
    myNeighbour = myNode->head;
    while (myNeighbour != NULL) {
        printf("neighbour: %ld\n", myNeighbour->neighbour->nodeNumber);
        myNeighbour = myNeighbour->next;
    }
}

int main(int argc, char* argv[]) {
    gNode* graphRoot = NULL;

    clock_t ttime;
    clock_t start;
    clock_t end;
    double time_taken;

    size_t i, j;

    pthread_t th[4];
    struct thread_args th_arguments[4];
    for (i = 0; i < 4; i++) {
        th_arguments[i].thread_number = i;
    }

    if (argv[2] == NULL || (strcmp(argv[2], "-t") != 0)) {
        printf("Run the program with \'-t\' flag and specify between 0 and 4 threads \n");
        return -1;
    }
    if (argv[3] == NULL || atoi(argv[3]) > 4) {
        printf("Please select between 0 and 4 threads \n");
        return -1;
    }
#ifdef TIME_OUTPUT
    printf("Forming Graph from Input file...\n");
#endif
    start = clock();
    if (formGraph(argv[1], &graphRoot) == -1) return -1;
    end = clock();
    time_taken = (double)(end - start) / CLOCKS_PER_SEC;
#ifdef TIME_OUTPUT
    printf("Graph formed!\nTime taken to read file and form Graph: %0.2lf seconds\n", time_taken);
    printf("List size = %ld\n", listSize);
#endif
    threadCount = atoi(argv[3]);

    sem_init(&semaphores[0], 0, 1);
    sem_init(&semaphores[1], 0, 1);
    sem_init(&semaphores[2], 0, 1);
    sem_init(&semaphores[3], 0, 1);
    sem_init(&main_semaphore, 0, 0);

    gNode* firstQuarter = graphRoot;
    gNode* firstThird = graphRoot;
    gNode* half = graphRoot;
    gNode* secondThird = graphRoot;
    gNode* thirdQuarter = graphRoot;

    size_t listQuarter = 0;
    size_t listThird = 0;
    size_t listHalf = 0;

    listHalf = listSize / 2;
    listThird = listSize / 3;
    listQuarter = listSize / 4;

    switch (threadCount) {
        case 1:
            th_arguments[0].fromNode = graphRoot;
            th_arguments[0].toNode = NULL;
            break;
        case 2:
            for (i = 0; i < listHalf; i++) {
                half = half->next;
            }
            th_arguments[0].fromNode = graphRoot;
            th_arguments[0].toNode = half;

            th_arguments[1].fromNode = half;
            th_arguments[1].toNode = NULL;

            break;
        case 3:
            for (i = 0; i < listSize; i++) {
                if (i < listThird)
                    firstThird = firstThird->next;
                if (i < 2 * listThird)
                    secondThird = secondThird->next;
            }

            th_arguments[0].fromNode = graphRoot;
            th_arguments[0].toNode = firstThird;

            th_arguments[1].fromNode = firstThird;
            th_arguments[1].toNode = secondThird;

            th_arguments[2].fromNode = secondThird;
            th_arguments[2].toNode = NULL;
            break;
        case 4:
            for (i = 0; i < listSize; i++) {
                if (i < listQuarter)
                    firstQuarter = firstQuarter->next;
                if (i < listHalf)
                    half = half->next;
                if (i < listHalf + listQuarter)
                    thirdQuarter = thirdQuarter->next;
            }

            th_arguments[0].fromNode = graphRoot;
            th_arguments[0].toNode = firstQuarter;

            th_arguments[1].fromNode = firstQuarter;
            th_arguments[1].toNode = half;

            th_arguments[2].fromNode = half;
            th_arguments[2].toNode = thirdQuarter;

            th_arguments[3].fromNode = thirdQuarter;
            th_arguments[3].toNode = NULL;
            break;

        default:
            th_arguments[0].fromNode = graphRoot;
            th_arguments[0].toNode = NULL;

            ttime = clock();

            pageRank(&graphRoot);

            ttime = clock() - ttime;
            time_taken = ((double)ttime) / CLOCKS_PER_SEC;
#ifdef TIME_OUTPUT
            printf("Time taken to run pagerank for %d itterations, without threads: %lf seconds\n", ITTERATIONS, time_taken);
#endif
            formOutput(graphRoot);

            return 1;
    }

    /* Create threads with the right arguments and run page rank with each thread */
    for (i = 0; i < threadCount; i++) {
        pthread_create(&th[i], NULL, par_pageRank, (void*)&th_arguments[i]);
    }
    start = clock();
    /**  
     * After every thread has calculated pagerank for their own part of the graph 
     * set the right page rank and allow threads to continue 
     * for ITTERATIONS amount of times
     */
    for (i = 0; i < ITTERATIONS; i++) {
        for (j = 0; j < threadCount; j++) {
            sem_wait(&main_semaphore);
        }
#ifdef DEBUG
        printf("All threads finished calculating, main process runs setPR\n");
#endif
        setPageRank(&graphRoot);
#ifdef DEBUG
        printf("Main process returned from setPR\n");
#endif
        for (j = 0; j < threadCount; j++) {
#ifdef DEBUG
            printf("Allowing thread(%d) to continue\n", j);
#endif
            sem_post(&semaphores[j]);
        }
    }
    end = clock();
    for (i = 0; i < threadCount; i++) {
        pthread_join(th[i], NULL);
    }

    time_taken = (double)(end - start) / CLOCKS_PER_SEC;
#ifdef TIME_OUTPUT
    printf("Time taken to run pagerank for %d itterations, with %d thread(s): %lf seconds\n", ITTERATIONS, threadCount, time_taken);
#endif
    formOutput(graphRoot);

    return 0;
}