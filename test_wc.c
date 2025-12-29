#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "mapreduce.h"

/* ---------------- Mapper ---------------- */

void WordCountMapper(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    if (!fp) {
        perror("fopen");
        return;
    }

    char word[1024];
    int idx = 0;
    int c;

    while ((c = fgetc(fp)) != EOF) {
        if (isalnum(c)) {
            word[idx++] = tolower(c);
        } else if (idx > 0) {
            word[idx] = '\0';
            MR_Emit(word, "1");
            idx = 0;
        }
    }

    // Emit last word if file does not end with delimiter
    if (idx > 0) {
        word[idx] = '\0';
        MR_Emit(word, "1");
    }

    fclose(fp);
}

/* ---------------- Reducer ---------------- */

void WordCountReducer(char *key, unsigned int partition_idx) {
    int count = 0;
    char *value;

    while ((value = MR_GetNext(key, partition_idx)) != NULL) {
        count += atoi(value);
        free(value);
    }

    printf("%s %d\n", key, count);
}

/* ---------------- Main ---------------- */

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <input files>\n", argv[0]);
        return 1;
    }

    unsigned int num_workers = 4;
    unsigned int num_partitions = 5;

    MR_Run(
        argc - 1,           // number of files
        &argv[1],           // file names
        WordCountMapper,    // mapper
        WordCountReducer,   // reducer
        num_workers,        // worker threads
        num_partitions      // partitions
    );

    return 0;
}