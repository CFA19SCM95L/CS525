CC = gcc
CFLAGS  = -g -Wall
 
default: run1

run1: test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test1 test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

run2: test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test2 test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

run_test1:
	./test1

run_test2:
	./test2

runandclean_1:
	run_test1 clean

runandclean_2:
	run_test2 clean

clean: 
	$(RM) test1 test2 *.o *~
