#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "threadpool.h"

// Map job wrapper argument passed to worker threads
typedef struct {
    Mapper mapper;      // Function pointer to the user's mapper function
    char* file_name;    // Duplicated file name string
} MapJobArg;

// Reducer job wrapper argument passed to worker threads
typedef struct {
    Reducer reducer;            // Function pointer to the user's reducer function
    unsigned int partition_idx; // Partition index to process
} ReduceJobArg;

// Allocated in MR_Run (based on num_parts) and cleaned up after

// Key/Value Node Structure
typedef struct kv_pair_t {
    char* key;                  // Dynamically allocated key string
    char* value;                // Dynamically allocated value string
    struct kv_pair_t* next;     // Pointer to next key-value pair in the linked list
} kv_pair_t;

// Partition Structure
typedef struct partition_t {
    kv_pair_t* head;        // Sorted linked list head
    unsigned int size;      // Size (number of pairs)
    pthread_mutex_t lock;   // Mutex for thread-safety during access
    kv_pair_t* current;     // Current pointer for MR_GetNext
} partition_t;

// Global Partition Variables
static partition_t* partitions = NULL;
static unsigned int partition_count = 0;

// Worker wrapper that calls the user's mapper & frees the wrapper
static void MapWrapper(void* arg) {
    // Check for NULL argument
    if (!arg) return;
    // Cast void* to MapJobArg* type
    MapJobArg* map_arg = (MapJobArg*) arg;

    // Call the user's mapper function
    if (map_arg->mapper) {
        map_arg->mapper(map_arg->file_name);
    }
    // Cleanup - Free the duplicated file name and the wrapper struct
    free(map_arg->file_name);
    free(map_arg);
}

// Main Functions

// MR_Run: Creates threadpool, submits map jobs, waits for completion, then submits reduce jobs, waits for completion, and cleans up
void MR_Run(unsigned int file_count, char* file_names[], Mapper mapper, Reducer reducer, unsigned int num_workers, unsigned int num_parts) {
    // Validate input parameters
    if (file_count == 0 || mapper == NULL || num_workers == 0) {
        return;
    }

    // Initialize partition data structures
    partition_count = num_parts;
    // Array of partitions
    partitions = malloc(sizeof(partition_t) * partition_count);
    if (!partitions) {
        fprintf(stderr, "Failed to allocate memory for partitions\n");
        return;
    }

    // Initialize each partition - head = NULL, size = 0, init mutex
    for (unsigned int i = 0; i < partition_count; i++) {
        partitions[i].head = NULL;
        partitions[i].current = NULL;
        partitions[i].size = 0;
        // Initialize and Validate mutex initialization for this partition
        // If mutex init fails, clean up previously initialized mutexes and free partitions
        if (pthread_mutex_init(&partitions[i].lock, NULL) != 0) {
            fprintf(stderr, "Failed to initialize mutex for partition %u\n", i);
            // Clean up previously initialized mutexes
            for (unsigned int j = 0; j < i; j++) {
                pthread_mutex_destroy(&partitions[j].lock);
            }
            free(partitions);
            partitions = NULL;
            partition_count = 0;
            return;
        }
    }

    // Create thread pool with given number of workers
    ThreadPool_t* tp = ThreadPool_create(num_workers);
    // If thread pool creation fails, clean up partitions and return
    if (!tp) {
        fprintf(stderr, "Failed to create thread pool\n");
        for (unsigned int i = 0; i < partition_count; i++) {
            pthread_mutex_destroy(&partitions[i].lock);
        }
        free(partitions);
        partitions = NULL;
        partition_count = 0;
        return;
    }

    // Map Phase - submit map jobs for each input file
    // ===============================================

    // Submit map jobs for each input file. Allocate a MapJobArg for each job
    for (unsigned int i = 0; i < file_count; i++) {

        // Determine file weight (file size) for SJF scheduling
        // st is used to get file size
        struct stat st;
        size_t file_weight = 0;
        // stat(file_names[i], &st) returns 0 on success ; &st is populated with file info
        if (stat(file_names[i], &st) == 0) {
            file_weight = (size_t) st.st_size;
        }

        // Allocate and populate MapJobArg
        MapJobArg* arg = (MapJobArg*) malloc(sizeof(MapJobArg));
        if (!arg) {
            fprintf(stderr, "Failed to allocate memory for MapJobArg\n");
            continue; // Skip this file
        }

        // Duplicate the file name string so MR_Run owns the memory for the job arg
        arg->file_name = strdup(file_names[i]);
        // If strdup fails, clean up and skip this file
        if (!arg->file_name) {
            fprintf(stderr, "Failed to duplicate file name string\n");
            free(arg->file_name);
            free(arg);
            continue; // Skip this file
        }
        // Set the mapper function pointer
        arg->mapper = mapper;

        // Submit map job to thread pool with file weight, threadpool will schedule based on weight - SJF Scheduling
        bool added = ThreadPool_add_job(tp, MapWrapper, (void*) arg, file_weight);
        // If adding job fails, clean up and skip this file
        if (!added) {
            fprintf(stderr, "MR_Run: ThreadPool_add_job failed for file %s\n", file_names[i]);
            free(arg->file_name);
            free(arg);
            continue; // Skip this file
        }    
    }
    
    // Wait for all map jobs to complete
    ThreadPool_check(tp);
    
    // Reduce phase - submit reduce jobs for each partition
    // ====================================================

    for (unsigned int i = 0; i < num_parts; i++) {
        // Skip empty partitions
        if (partitions[i].size == 0) continue;

        // Allocate and populate ReduceJobArg
        ReduceJobArg* arg = malloc(sizeof(ReduceJobArg));
        // If allocation fails, skip this partition
        if (!arg) {
            fprintf(stderr, "Failed to allocate memory for ReduceJobArg\n");
            continue; // Skip this partition
        }
        // Populate ReduceJobArg
        arg->reducer = reducer;
        arg->partition_idx = i;

        // Submit reduce job for this partition
        bool added = ThreadPool_add_job(tp, MR_Reduce, (void*) arg, partitions[i].size);
        // If adding job fails, clean up and skip this partition
        if (!added) {
            fprintf(stderr, "Failed to add reduce job for partition %u\n", i);
            free(arg);
            continue; // Skip this partition
        }
    }

    // Wait for all reduce jobs to complete
    ThreadPool_check(tp);

    // Destroy the thread pool
    ThreadPool_destroy(tp);

    // Cleanup partition data structures
    for (unsigned int i = 0; i < num_parts; ++i) {
        // Free all key-value pairs in the partition
        kv_pair_t* current = partitions[i].head;
        // While there are still key-value pairs, free them
        while (current) {
            kv_pair_t* temp = current;
            current = current->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
        // Destroy the mutex for this partition
        pthread_mutex_destroy(&partitions[i].lock);
        // Set pointers to NULL to avoid dangling pointers
        partitions[i].head = NULL;
        partitions[i].current = NULL;
    }
    // Free the partition structure itself
    free(partitions);
    partitions = NULL;
    partition_count = 0;
}

// MR_Emit - called by mapper to emit key-value pairs to appropriate partition
void MR_Emit(char* key, char* value) {
    
    // Validate key and partitions
    if (!key || !value) return;
    if (strlen(key) == 0) return;
    if (!partitions) return;

    // Determine partition index using the partitioner function
    unsigned int partition_idx = MR_Partitioner(key, partition_count);
    // Get the partition for the determined index
    partition_t* partition = &partitions[partition_idx];

    // Lock the partition while inserting the new key-value pair
    pthread_mutex_lock(&partition->lock);

    // Create a new key-value pair & set its fields
    kv_pair_t* new_pair = malloc(sizeof(kv_pair_t));
    // If allocation fails, unlock and return
    if (!new_pair) {
        pthread_mutex_unlock(&partition->lock);
        return;
    }
    
    // Initialize the new pair
    new_pair->next = NULL;
    new_pair->key = strdup(key);
    // If strdup fails, clean up and unlock
    if (!new_pair->key) {
        free(new_pair);
        pthread_mutex_unlock(&partition->lock);
        return;
    }
    // Duplicate the value string
    new_pair->value = strdup(value);
    // If strdup fails, clean up and unlock
    if (!new_pair->value) {
        free(new_pair->key);
        free(new_pair);
        pthread_mutex_unlock(&partition->lock);
        return;
    }

    // Insert in sorted order in partitions based on key

    // current is pointer to a pointer to kv_pair_t 
    kv_pair_t** current = &partition->head;
    // While current is not NULL and current key is less than new key, move to next
    while (*current != NULL && strcmp((*current)->key, key) < 0) {
        current = &((*current)->next);
    }

    // Insert the new pair at the found position
    new_pair->next = *current; // New node points to current node
    *current = new_pair;       // Previous node (or head) points to new node

    partition->size++;         // Increment partition size
    pthread_mutex_unlock(&partition->lock);
}

// MR_Partitioner - hash function to determine partition index
unsigned int MR_Partitioner(char* key, unsigned int num_partitions) {
    // If key is NULL or num_partitions is 0, return 0
    if (!key || num_partitions == 0) {
        return 0; // Default partition
    }
    // DJB2 hash function  
    unsigned long hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return (unsigned int)((hash * 31) % num_partitions);
}

/**
 * This function will run in a worker thread to process a partition
 * threadarg is a pointer to a ReduceJobArg struct - allocated in MR_Run when submitting reduce jobs
 */
void MR_Reduce(void* threadarg) {
    // If threadarg is NULL, return
    if (!threadarg) return;

    // Cast to ReduceJobArg and extract fields
    ReduceJobArg* arg = (ReduceJobArg*) threadarg;
    Reducer reducer = arg->reducer;
    unsigned int partition_idx = arg->partition_idx;

    // Validate partition index
    if (partition_idx >= partition_count) { free(arg); return; }
    if (!partitions) { free(arg); return; }

    // Get the partition
    partition_t* partition = &partitions[partition_idx];

    // Initialize the current pointer for MR_GetNext
    kv_pair_t* iterator = partition->head;
    // Keep track of the last key seen
    char* last_key = NULL;

    // Iterate through the partition's key-value pairs, grouping by unique keys, keys were already sorted during insertion
    while (iterator != NULL) {
        // If this key is different from the last key, we have a new unique key
        if (last_key == NULL || strcmp(iterator->key, last_key) != 0) {

            // Set the current pointer in the partition for MR_GetNext
            pthread_mutex_lock(&partition->lock);
            partition->current = iterator;
            pthread_mutex_unlock(&partition->lock);

            // Call the user's reducer function with the current key and partition index
            if (reducer) {
                reducer(iterator->key, partition_idx);
            }

            // Update last_key to the current key
            last_key = iterator->key;
        }
        // Move to the next key-value pair
        iterator = iterator->next;
    }
    free(arg); // Free the ReduceJobArg struct
}

/**
 * Called by the user's reducer to get the next value of the given key from the partition
 * Returns a newly-allocated copy of the value string, or NULL if no more values
 * Note for myself: The caller is responsible for freeing the returned string
 */
char* MR_GetNext(char* key, unsigned int partition_idx) {
    // If key is NULL or partition_idx is invalid, return NULL
    if (!key) return NULL;
    // If partitions_idx is out of bounds, return NULL
    if (partition_idx >= partition_count) return NULL;
    // Get the partition
    partition_t* partition = &partitions[partition_idx];

    pthread_mutex_lock(&partition->lock);
    // Get the current key-value pair
    kv_pair_t* current = partition->current;
    // If current is NULL, no more key-value pairs
    if (!current) {
        pthread_mutex_unlock(&partition->lock);
        return NULL; // No more key-value pairs
    }

    // Only return values that match the requested key
    // If the current key matches the requested key, return the value
    if (strcmp(current->key, key) == 0) {
        // Key matches, return the value and advance the current pointer
        char* value_copy = strdup(current->value);
        partition->current = current->next;
        pthread_mutex_unlock(&partition->lock);
        return value_copy;
    } else {
        // Key does not match ; no more values for this key
        // I don't advance the current pointer here - already handled in MR_Reduce
        pthread_mutex_unlock(&partition->lock);
        return NULL;
    }
}