CC = gcc
CFLAGS  = -g -Wall
 
default: test2_1

test2_1: test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test1 test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

test2_2: test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test2 test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

run_test2_1:
	./test2_1

run_test2_2:
	./test2_2

runandclean_1:
	run_test2_1 clean

runandclean_2:
	run_test2_2 clean

clean: 
	$(RM) test2_1 test2_2 *.o *~
