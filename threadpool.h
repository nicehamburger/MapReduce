#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>

/** Function Pointer Type Definition
 * Define a function pointer type for thread functions
 * thread_func_t is a pointer to a function that takes a void* argument and returns void
 * Allows us to use thread_func_t to represent any function that matches this signature
 * Can effectively then pass different functions as jobs to the thread pool
 * Note: *void - special type of pointer that can point to any data type
 */
typedef void (*thread_func_t)(void* arg);

/** Job Structure
 * ThreadPool_job_t represents a single job in the thread pool's job queue
 */
typedef struct ThreadPool_job_t {
    thread_func_t func;             // points to the function the thread will execute
    void* arg;                      // points to the data passed to the function (arguments)
    struct ThreadPool_job_t* next;  // points to the next job in the queue
    size_t weight;                  // job size (SJF scheduling)
} ThreadPool_job_t;

/** Job Queue Structure
 *  head -> [func=task1, weight=5] -> [func=task2, weight=10] -> [func=task3, weight=2] -> NULL
 */
typedef struct {
    unsigned int size;       // no. of jobs in the queue
    ThreadPool_job_t* head;  // points to the first job in the queue - the job with the highest priority (smallest weight)
} ThreadPool_job_queue_t;

/** Thread Pool Structure
 * Array of worker threads, Job Queue, Synchronization Primitives, Control Flag and Cleanup
 */
typedef struct {
    pthread_t* threads;           // Pointer to the array of thread handles
    unsigned int num_threads;     // Number of threads in the pool
    
    unsigned int idle_count;      // Number of threads currently idle ; might help with monitoring & scheduling
    bool shutdown;                // Flag to tell threads to exit ; when true, tells all threads to stop gracefully
    
    pthread_cond_t cond;          // Signaled when new job arrives or shutdown
    pthread_mutex_t lock;         // Protects data from race conditions ; protects the queue, idle_count, shutdown

    ThreadPool_job_queue_t jobs;  // Queue of jobs waiting for a thread to run
} ThreadPool_t;

// Function Declarations
// =====================

/** Create ThreadPool Object
* Constructor to create and initialize a ThreadPool object
* Parameters: num - Number of threads in the pool
* Return: Pointer to the created ThreadPool object, or NULL on failure
*/
ThreadPool_t* ThreadPool_create(unsigned int num);

/** Destroy ThreadPool Object
* Destructor to clean up and free resources used by the ThreadPool object
* Parameters: tp - Pointer to the ThreadPool object to be destroyed
*/
void ThreadPool_destroy(ThreadPool_t* tp);

/** Add Job to ThreadPool
* Parameters:
*     tp     - Pointer to the ThreadPool object
*     func   - Pointer to the function that will be called by the serving thread
*     arg    - Arguments for that function
*     weight - Job weight required for SJF scheduling
* Return:
*     true   - On success
*     false  - Otherwise
*/
bool ThreadPool_add_job(ThreadPool_t* tp, thread_func_t func, void* arg, size_t weight);

/** Get Next Job from ThreadPool
* Parameters: tp - Pointer to the ThreadPool object
* Return: ThreadPool_job_t* - Next job to run
*/
ThreadPool_job_t* ThreadPool_get_job(ThreadPool_t* tp);

/** Main Worker Thread Function
 * Start routine for each worker thread in the ThreadPool
 * 
 * Each thread remains active until the ThreadPool is signaled to shutdown
 * Continuously monitors the job queue in a loop, retrieving and executing available jobs
 * 
 * ThreadPool is a user level abstraction - pthreads is not aware of threadpool existence
 * ThreadPool Object (pool)
 *          │
 *          |-- Thread 1: running Thread_run(pool)
 *          |-- Thread 2: running Thread_run(pool)
 *          |-- Thread 3: running Thread_run(pool)
 *
 * All threads pull jobs from the SAME queue in pool
 * 
 * Parameters:
 *     arg - Void pointer that should be cast to ThreadPool_t*, representing the ThreadPool instance that owns this worker thread
 * 
 * Returns:
 *     NULL upon thread termination (signaled by ThreadPool shutdown)
 * 
 * Note:
 * pthread_create API expect void* (*start_routine)(void*) - a function that takes exactly void* and returns void*
 */
void* Thread_run(void* arg);

/** Wait for All Jobs to Complete
* Ensure that all threads are idle and the job queue is empty before returning
* Parameter: tp - Pointer to the ThreadPool object
*/
void ThreadPool_check(ThreadPool_t* tp);

#endif
