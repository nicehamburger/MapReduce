# MapReduce Library

A multi-threaded **MapReduce** library implemented in **C**, designed for parallel data processing. This project leverages a custom-built **thread pool** that uses **Shortest Job First (SJF)** scheduling to optimize task throughput.

## Architecture

The framework consists of two main components:

### 1. Thread Pool (`threadpool.c`)
- Creates a pool of worker threads.
- Jobs are queued using **SJF scheduling** (shortest job first).
- Worker threads execute tasks from the queue and handle shutdown gracefully.
- Supports:
  - Adding jobs (`ThreadPool_add_job`)
  - Fetching jobs (`ThreadPool_get_job`)
  - Waiting for job completion (`ThreadPool_check`)
  - Pool destruction (`ThreadPool_destroy`)

### 2. MapReduce (`mapreduce.c`)
- Provides a MapReduce interface built on the custom thread pool.
- **Map Phase**
  - Submits a map job for each input file.
  - Each map job calls the user-defined `Mapper` function.
  - Key-value pairs emitted via `MR_Emit` are inserted into sorted, thread-safe partitions.
- **Reduce Phase**
  - Submits a reduce job for each partition with at least one key-value pair.
  - Reducer function processes unique keys using `MR_GetNext` to fetch values.
- **Partitioning**
  - Uses `MR_Partitioner` (hash function) to distribute keys across partitions.
  - Each partition has a mutex for safe concurrent access.

## Getting Started

### Prerequisites
- GCC or any C compiler supporting **POSIX threads**.
- Unix-like OS recommended (Linux, macOS).

### Compilation
To compile the library and the test program using the provided Makefile
```bash
make
```
Pass the files you wish to process as command-line arguments
```bash
./test_wc file1.txt file2.txt
```
To remove object files and the executable
```bash
make clean
```

## Usage
To use API for your own program
1. Define Mapper and Reducer. 
   You must implement the logic for your specific data processing task:
```
void my_mapper(char* file_name) {
    // Open file, parse data, and call MR_Emit(key, value)
}

void my_reducer(char* key, unsigned int partition_idx) {
    // Use MR_GetNext(key, partition_idx) to process values
}
```
2. Run the Framework. 
   Call MR_Run from your main function:
```
MR_Run(file_count, file_names, my_mapper, my_reducer, num_workers, num_partitions);
```
3. main.c is your driver program that provides mapper and reducer functions.
```bash
gcc -pthread -o mapreduce mapreduce.c threadpool.c main.c
```

## Notes
- Ensure your Mapper and Reducer functions are thread-safe if using shared resources.
- Optimal performance depends on correctly tuning the number of worker threads (num_workers) and the number of partitions required.
- All intermediate data is automatically cleaned up after the Reduce phase, but careful memory management in Mapper and Reducer is recommended.

## Acknowledgements

The `mapreduce.h` header and associated scaffolding were provided as part of the CMPUT 379: Operating Systems Concepts course at the University of Alberta.  
All threading logic, SJF scheduling, and MapReduce implementation beyond the provided header were developed by the project author.
