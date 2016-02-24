#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>

// Represents a word in a given buffer
typedef struct word_node {
	char *word; // the word represented by a string
	int count; // the number of times the word appears
} word_node;


// Buffer object that is shared among threads
typedef struct buffer_t {
	int size; // the number of word nodes in the buffer
	word_node *words; // array of all the word nodes in the buffer
	int replica; // which replica number the buffer belongs to
	bool completed; // flag indicating when the buffer is done being added to
} buffer_t;


// Structure that holds the arguments for map-reader-i's creation
typedef struct map_reader_args {
	buffer_t *buffer;
} map_reader_args;


// Structure that holds the arguments for map-adder-i's creation
typedef struct map_adder_args {
	buffer_t *buffer1;
	buffer_t *buffer2;
} map_adder_args;


// Structure that holds the arguments for reducer's creation
typedef struct map_reduce_args {
	buffer_t **buffer;
} map_reduce_args;


// Functions of the program that are defined in the .c file
char *fileToString(char *filename);
int checkIndex(int index, char* string);
bool all_threads_complete(buffer_t *buffer1[], buffer_t *buffer2[]);
int count_words(const char *sentence);
void *map_reader(void *arguments);
void *map_adder(void *arguments);
void *map_reduce(void *arguments);
