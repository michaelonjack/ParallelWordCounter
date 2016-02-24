#include "word_counter.h"

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

// The maximum size of the buffer shared by reader-i and adder-i
int buf_max_size;
// The size of the input file in bytes
int file_size;
// The number of replicas
int n;
// String containing the contents of the input file
char *contents;
// The mutex lock used for mutual exclusion across the program
pthread_mutex_t lock;

int main(int argc, char **argv) {
	
	// Require 4 command-line arguments to run the program
	if (argc >= 4) {
		// Convert the input file to a string (the file's name is a command line argument)
		contents = fileToString(argv[1]);
		// Get the size of the file in bytes
		file_size = strlen(contents);
		// Get the number of replicas that the program will use (input to program)
		n = atoi(argv[2]);
		// Get the maximum size of the buffer shared by reader-i and adder-i (input to program)
		buf_max_size = atoi(argv[3]);
		
		int num_words = count_words(contents);
		
		// Buffer shared by reader-i and adder-i
		buffer_t *buffer1;
		// Buffer shared by adder-i and reducer-i
		buffer_t *buffer2;
		// Array of all the buffers shared by the readers and adders
		buffer_t *reader_adder_buffers[n];
		// Array of all the buffers shared by the adders and the reducer
		buffer_t *adder_reducer_buffers[n];
		
		// Initialize the mutex lock and ensure that the initialization was successful
		int rc = pthread_mutex_init(&lock, NULL);
		assert(rc == 0);
	
		// Loop to create the n replicas
		for (int i=1; i <= n; i++) {
			// Create threads reader-i and adder-i
			pthread_t *map_reader_thread = malloc(sizeof(map_reader_thread));
			pthread_t *map_adder_thread = malloc(sizeof(map_adder_thread));
			
			// Allocate space for the arguments of the reader-i and adder-i threads
			map_reader_args *reader_args = malloc(sizeof(reader_args));
			map_adder_args *adder_args = malloc(sizeof(adder_args));;
			
			// Initialize the buffer shared by the reader-i and adder-i threads
			buffer1 = malloc(sizeof(buffer_t));
			buffer1->size = 0;
			buffer1->words = malloc(buf_max_size * sizeof(word_node));
			buffer1->replica = i;
			buffer1->completed = false;
			
			// Initialize the buffer shared by the adder-i and reducer threads
			buffer2 = malloc(sizeof(buffer_t));
			buffer2->size = 0;
			buffer2->words = malloc(sizeof(word_node) * num_words * 2);
			buffer2->replica = i;
			buffer2->completed = false;
			
			// Set the arguments for the reader-i and adder-i threads
			reader_args->buffer = buffer1;
			
			adder_args->buffer1 = buffer1;
			adder_args->buffer2 = buffer2;
			
			// Save the buffers in their respective arrays
			reader_adder_buffers[i-1] = buffer1;
			adder_reducer_buffers[i-1] = buffer2;
			
			// Create the reader-i and adder-i threads
			pthread_create(map_reader_thread, NULL, map_reader, reader_args);
			pthread_create(map_adder_thread, NULL, map_adder, adder_args);
		}
		
		// Busy wait until all reader and adder threads are completed
		while( !all_threads_complete(reader_adder_buffers, adder_reducer_buffers) );
		
		// Create the reducer thread
		pthread_t map_reduce_thread;
		// Create and initialize the parameter structure for the reducer thread
		map_reduce_args reduce_args;
		reduce_args.buffer = adder_reducer_buffers;
		// Create the reducer thread and send it to the map_reduce function
		pthread_create(&map_reduce_thread, NULL, map_reduce, &reduce_args);
		// Wait for reducer thread to complete before exiting the main thread
		pthread_join(map_reduce_thread, NULL);
		
		// Destroy the mutex lock because it is no longer needed
		pthread_mutex_destroy(&lock);
	}
	
	// If the user did not provide 4 command line arguments, prompt them to
	else {
		printf("%s\n", "Three args needed:\n\t1.) input file name\n\t2.) buffer size\n\t3.) number of replicas\n\tExample: ./out input.txt 20 3\n");
	}
	
	return 0;
}






////////////////////////////////////////////////////////////////////////////////
//
// Function     : map_reader
// Description  : function to be completed by the map-reader-i threads. Reads portion i of the file
//				  and adds each word it encounters to the parameter buffer with a count of 1
//
// Inputs       : buffer - the buffer of words shared by map-reader-i and map-adder-i
//
// Outputs      : None
//	
void* map_reader(void *arguments) {
	
	// Unpack the arguments from the argument structure
	map_reader_args *args = (map_reader_args *)arguments;
	buffer_t *buffer = args->buffer;
	int i = buffer->replica;
	
	// Starting index of the portion of the input file that replica i is responsible for
	int start = ((i-1)*file_size)/n;
	start = checkIndex(start, contents);
	// Ending index of the portion of the input file that replica i is responsible for
	int end = (i*file_size) / n;
	end = checkIndex(end, contents);
	// String of deliminators used to isolate the words of the input file
	char *delim = " ;:!?'\"(),.-\n\t\r";
	
	// Create the substring of the input file that replica i is responsible for
	char *substr = malloc(end-start+1);
	memcpy(substr, &contents[start], end-start);
	substr[end-start] = '\0';
	
	// Grab individual words from the substring and add them to the buffer by tokenizing the substring
	char *saveptr;
	char *word = strtok_r(substr, delim, &saveptr);
	while (word != NULL) {
		
		// Busy wait while the buffer is full
		while( buffer->size >= buf_max_size );
		
		// Create the new word node to be added to the budder
		word_node new_word;
		new_word.count = 1;
		new_word.word = word;
		
		// Acquire the lock and add the new word to the buffer and increase its size
		pthread_mutex_lock(&lock);
		buffer->words[buffer->size] = new_word;
		buffer->size++;
		pthread_mutex_unlock(&lock);
		
		// Get the next word from the substring
		word = strtok_r(NULL, delim, &saveptr);
	}
	
	// Once replica i is done processing the file, set the buffer to complete
	buffer->completed = true;
	return NULL;
}








////////////////////////////////////////////////////////////////////////////////
//
// Function     : map_adder
// Description  : function to be completed by the map-adder-i threads. Reads the entries of buffer1
//				  produced by map-reader-i and combines the similar entries into one entry in buffer2
//
// Inputs       : buffer1 - buffer that is shared by map-reader-i and map-adder-i
//				  buffer2 - buffer that is shared by map-adder-i and the reducer
//
// Outputs      : None
//	
void* map_adder(void *arguments) {
	
	// Flag to indicate if the current word has been added to buffer2
	int added;
	
	// Unpack the arguments from the map_adder_args structure
	map_adder_args *args = (map_adder_args *)arguments;
	buffer_t *buffer1 = args->buffer1;
	buffer_t *buffer2 = args->buffer2;
	
	do {
		
		// Busy wait if there is nothing left to consume
		while(buffer1->size < 1 && !buffer1->completed);
		
		// Acquire the lock before adding to buffer2
		pthread_mutex_lock(&lock);
		// Add the word from buffer1 to buffer2 if it has not yet been added
		// If it has already been added, increase its word count
		for (int i = buffer1->size-1; i >= 0; i--) {
			added = false;
			// Determine if the new word is already in the second buffer
			for (int j = 0; j < buffer2->size; j++) {
				if ( strcmp(buffer1->words[i].word, buffer2->words[j].word) == 0 ) {
					// If already in second buffer, increase word count
					buffer2->words[j].count++;
					added = true;
					break;
				}
			}
		
			// If not yet added to the second buffer, add it and increase the buffer size
			if (!added) {
				buffer2->words[buffer2->size] = buffer1->words[i];
				buffer2->size++;
			}
		
			// Decrease the size of buffer1to show that entries have been consumed
			buffer1->size--;
		}
		pthread_mutex_unlock(&lock);
		
	} while(!buffer1->completed || buffer1->size > 0);
	
	// Once all of buffer1's entries have been consumed, set buffer2 to completed
	buffer2->completed = true;
	
	return NULL;
}








////////////////////////////////////////////////////////////////////////////////
//
// Function     : map_reduce
// Description  : Reads all n buffers produced by each map-adder-i and creates a final count for all
//				  unique words in the file. Creates an output file containing all of these word, count pairs
//
// Inputs       : buffer - an array containing all the buffers produced by map-adder-i for all i
//
// Outputs      : None
//	
void *map_reduce(void *arguments) {
	
	// Unpack arguments from the map_reduce_args structure
	map_reduce_args *args = (map_reduce_args *)arguments;
	buffer_t **buffer = args->buffer;
	
	FILE *output = fopen("output.txt", "a+");
	
	char *current_word;
	int current_count;
	RESTART:
	current_word = NULL;
	current_count = 0;
	// Iterate through the map-adder buffers
	for (int i=0; i < n; i++) {
		// Iterate through the words of the current map-adder buffer
		for (int j=0; j < buffer[i]->size; j++) {				
			if (current_word == NULL) {
				if ( buffer[i]->words[j].word != NULL ) {
					current_word = buffer[i]->words[j].word;
					current_count = buffer[i]->words[j].count;
					buffer[i]->words[j].word = NULL;
				}
			}
			else if (buffer[i]->words[j].word != NULL) {
				if ( strcmp(current_word, buffer[i]->words[j].word) == 0 ) {
					current_count += buffer[i]->words[j].count;
					buffer[i]->words[j].word = NULL;
				}
			}
		}
	}
	
	if (current_word != NULL) {
		fprintf(output, "%s %d\n", current_word, current_count);
		goto RESTART;
	}
	
	fclose(output);

	return NULL;
}






////////////////////////////////////////////////////////////////////////////////
//
// Function     : fileToString
// Description  : Converts the provided text file to a string
//
// Inputs       : filename - the name of the file we want to convert to a string
//
// Outputs      : the char* variable that holds the file's contents as a string
//				  
char *fileToString(char *filename) {
	int size;
	char *contents = 0;
	
	// Open the file
	FILE *fp = fopen(filename, "r");
	
	// Find size of the file in bytes
	fseek(fp, 0L, SEEK_END);
	size = ftell(fp);
	rewind(fp);
	
	// Allocate memory for the string containing the file's contents
	contents = malloc(size);
	if (contents) {
		fread(contents, 1, size, fp);
	}
	contents[size-1] = '\0';
	
	// Close the file
	fclose(fp);
	
	return contents;
}






////////////////////////////////////////////////////////////////////////////////
//
// Function     : checkIndex
// Description  : Checks if the start or end index of the string for replica i lies in the middle of a word
//				  If so, move the index to the start of the next word
//
// Inputs       : index - the index into the string we are checking
//				  string - the string which our index applies to
//
// Outputs      : the correct value for the index (not in the middle of a word
//	
int checkIndex(int index, char* string) {
	
	if (index != 0) {
		while (string[index] != ' ' && string[index] != '\0') {
			index++;
		}
	}
	
	return index;
}






////////////////////////////////////////////////////////////////////////////////
//
// Function     : all_threads_complete
// Description  : Returns true if all of the parameter buffers have completed
//
// Inputs       : buffer1[] - first set of buffers to check for completion
//				  buffer2[] - second set of buffers to check for completion
//
// Outputs      : true if all buffers are completed, false otherwise
//	
bool all_threads_complete(buffer_t *buffer1[], buffer_t *buffer2[]) {
	for( int i=0; i < n; i++ ) {
		if ( (buffer1[i]->completed == false) || (buffer2[i]->completed == false) ) {
			return false;
		}
	}
	
	return true;
}





////////////////////////////////////////////////////////////////////////////////
//
// Function     : count_words
// Description  : counts number of words in the file
//
// Inputs       : string - the string whose words we will count
//
// Outputs      : the number of words
//
int count_words(const char *string)
{
    int count,i,len;
    char lastC;
    len=strlen(string);
    if(len > 0)
    {
        lastC = string[0];
    }
    for(i=0; i<=len; i++)
    {
        if(string[i]==' ' && lastC != ' ')
        {
            count++;
        }
        lastC = string[i];
    }
    if(count > 0 && string[i] != ' ')
    {
        count++;
    }
    return count;
}
