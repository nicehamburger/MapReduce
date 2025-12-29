#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "threadpool.h"

// Helper function to create a new job
static ThreadPool_job_t* job_create(thread_func_t func, void* arg, size_t weight) {
    // Allocate memory for a new job
    ThreadPool_job_t* job = malloc(sizeof(ThreadPool_job_t));
    // Check if memory allocation was successful
    if (!job) return NULL;
    // Initialize the job fields ; job->func = (*job).func;
    job->func   = func;
    job->arg    = arg;
    job->next   = NULL;
    job->weight = weight;
    return job;
}

// Create a new thread pool with num threads
ThreadPool_t* ThreadPool_create(unsigned int num) {
    // Check for valid number of threads
    if (num == 0) return NULL;

    // Allocate memory for the ThreadPool structure
    ThreadPool_t* tp = malloc(sizeof(ThreadPool_t));
    if (!tp) return NULL;

    // Initialize ThreadPool fields
    tp->num_threads = num;
    // Allocate memory for the array of thread handles
    tp->threads = malloc(sizeof(pthread_t) * num);
    // Check if memory allocation for threads was successful
    if (!tp->threads) { free(tp); return NULL; }

    tp->idle_count = 0;
    tp->shutdown   = false;
    tp->jobs.head  = NULL;
    tp->jobs.size  = 0;

    // Initialize mutex and condition variable

    // pthread_mutex_t lock; Mutual exclusion lock
    if (pthread_mutex_init(&tp->lock, NULL) != 0) {
        // Cleanup on failure
        free(tp->threads);
        free(tp);
        return NULL;
    }

    // pthread_cond_t cond; Condition variable
    if (pthread_cond_init(&tp->cond, NULL) != 0) {
        // Cleanup on failure
        pthread_mutex_destroy(&tp->lock);
        free(tp->threads);
        free(tp);
        return NULL;
    }

    // Create worker threads
    for (unsigned int i = 0; i < num; i++) {
        // Create a new thread with Thread_run as the start routine and 'tp' as the argument
        int t_rc = pthread_create(&tp->threads[i], NULL, Thread_run, (void*) tp);
        if (t_rc != 0) {
            // Cleanup on failure, set shutdown flag
            pthread_mutex_lock(&tp->lock);
            tp->shutdown = true;
            // Signal all threads to exit
            pthread_cond_broadcast(&tp->cond);
            pthread_mutex_unlock(&tp->lock);
            // Join already created threads
            for (unsigned int j = 0; j < i; j++) {
                pthread_join(tp->threads[j], NULL);
            }
            // Destroy mutex and condition variable
            pthread_cond_destroy(&tp->cond);
            pthread_mutex_destroy(&tp->lock);
            // Free allocated memory
            free(tp->threads);
            // Free the ThreadPool structure
            free(tp);
            return NULL;
        }
    }
    return tp;
}

// Add a new job to the thread pool, weight is used for SJF scheduling
bool ThreadPool_add_job(ThreadPool_t* tp, thread_func_t func, void* arg, size_t weight) {
    // Validate input parameters
    if (!tp || !func) return false;

    // Create a new job
    ThreadPool_job_t* new_job = job_create(func, arg, weight);
    if (!new_job) return false;

    // Lock the thread pool to protect shared data (Critical Section)
    // Modifying shared data: job  queue, job count, etc.
    pthread_mutex_lock(&tp->lock);

    // If the pool is shutting down, clean up and return failure
    if (tp->shutdown) {
        free(new_job);
        pthread_mutex_unlock(&tp->lock);
        return false;
    }

    // Insert the new job into the job queue using SJF (Shortest Job First) scheduling
    // If the queue is empty or the new job has the smallest weight, insert at head
    if (tp->jobs.head == NULL || new_job->weight < tp->jobs.head->weight) {
        new_job->next = tp->jobs.head;
        tp->jobs.head = new_job;
    } else {
        // Traverse the queue to find the correct position based on weight
        ThreadPool_job_t* current = tp->jobs.head;
        // While next job exists and its weight is less than or equal to new job's weight
        while (current->next != NULL && current->next->weight <= new_job->weight) {
            // Move to the next job
            current = current->next;
        }
        // Insert the new job in its correct position
        new_job->next = current->next; // New job points to the (old) next job
        current->next = new_job;       // Current job points to the new job
    }

    // Update the job count
    tp->jobs.size++;

    // Signal a worker thread that a new job is available
    pthread_cond_signal(&tp->cond);
    // Unlock the thread pool
    pthread_mutex_unlock(&tp->lock);
    return true;
}

// Get the next job from the thread pool
ThreadPool_job_t* ThreadPool_get_job(ThreadPool_t* tp) {
    // Validate input parameter
    if (!tp) return NULL;

    // Acquire lock to protect shared job queue data
    pthread_mutex_lock(&tp->lock);

    // Wait while there are no jobs available AND the pool isn't shutting down
    // pthread_cond_wait automatically releases the lock while waiting and
    // re-acquires it when signaled
    while (tp->jobs.head == NULL && !tp->shutdown) {
        tp->idle_count++;
        pthread_cond_broadcast(&tp->cond);
        pthread_cond_wait(&tp->cond, &tp->lock);
        tp->idle_count--;
    }
    
    // Check if the pool is shutting down
    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->lock);
        return NULL;  // Signal worker thread to exit
    }

    // Extract the next job from the front of the queue
    ThreadPool_job_t* job = tp->jobs.head;
    if (job) {
        // Update queue head to point to next job
        tp->jobs.head = job->next;
        // Decrement job count
        tp->jobs.size--;
    }
    
    // Release lock - job extraction is complete
    pthread_mutex_unlock(&tp->lock);
    
    // Return the job to the caller (worker thread)
    // Caller is responsible for executing the job and freeing memory
    return job;
}

// Worker Thread Main Loop
void* Thread_run(void* arg) {

    // Cast argument to ThreadPool pointer type
    ThreadPool_t* tp = (ThreadPool_t*) arg;
    if (!tp) return NULL;

    while (1) {
        
        // ThreadPool_get_handle handles all the locking and waiting logic
        ThreadPool_job_t* job = ThreadPool_get_job(tp);

        if (!job) {
            // Null means shutdown - exit thread
            break;
        }

        // Execute the Job (No locks held - allows parallelism)
        job->func(job->arg);
        // Job struct ownership belongs to ThreadPool, free it here
        free(job);

        // Notify the main thread (or anyone waiting) that a job is finished
        pthread_mutex_lock(&tp->lock);
        pthread_cond_broadcast(&tp->cond);
        pthread_mutex_unlock(&tp->lock);
    }
    return NULL;
}

// Wait until job queue empty and all threads idle
void ThreadPool_check(ThreadPool_t* tp) {
    if (!tp) return;

    pthread_mutex_lock(&tp->lock);
    // Don't wait if shutting down
    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->lock);
        return;
    }

    // Keep waiting until - no jobs left and all threads are idle
    while (!(tp->jobs.size == 0 && tp->idle_count == tp->num_threads)) {
        // Sleep until something changes (job added/completed)
        pthread_cond_wait(&tp->cond, &tp->lock);
    }
    pthread_mutex_unlock(&tp->lock);
}

// Destroy ThreadPool - Signal shutdown, Join Threads, Free Resources
void ThreadPool_destroy(ThreadPool_t* tp) {
    if (!tp) return;

    // Telling all threads to shutdown
    pthread_mutex_lock(&tp->lock);
    tp->shutdown = true;
    pthread_cond_broadcast(&tp->cond);  // Wake up all sleeping threads
    pthread_mutex_unlock(&tp->lock);

    // Waiting for all threads to finish
    for (unsigned int i = 0; i < tp->num_threads; ++i) {
        pthread_join(tp->threads[i], NULL);
    }

    // Cleaning up any leftover jobs
    pthread_mutex_lock(&tp->lock);
    ThreadPool_job_t* cur = tp->jobs.head;
    while (cur) {
        ThreadPool_job_t* next = cur->next;
        free(cur);  // Free job container
        cur = next;
    }
    tp->jobs.head = NULL;
    tp->jobs.size = 0;
    pthread_mutex_unlock(&tp->lock);

    // Free all resources
    pthread_cond_destroy(&tp->cond);
    pthread_mutex_destroy(&tp->lock);
    free(tp->threads);
    free(tp);
}