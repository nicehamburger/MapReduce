CC = gcc
CFLAGS = -Wall -pthread

OBJS = mapreduce.o threadpool.o test_wc.o

all: test_wc

test_wc: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o test_wc

mapreduce.o: mapreduce.c mapreduce.h threadpool.h
	$(CC) $(CFLAGS) -c mapreduce.c

threadpool.o: threadpool.c threadpool.h
	$(CC) $(CFLAGS) -c threadpool.c

test_wc.o: test_wc.c mapreduce.h
	$(CC) $(CFLAGS) -c test_wc.c

clean:
	rm -f *.o test_wc