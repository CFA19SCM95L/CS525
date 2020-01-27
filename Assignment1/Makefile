CC = gcc
CFLAGS  = -g -Wall 

launch: test_assign1_1.o storage_mgr.o dberror.o 
	$(CC) $(CFLAGS) -o test_assign1 test_assign1_1.o storage_mgr.o dberror.o -lm

clean: 
	$(RM) launch *.o *~

run: launch
	./test_assign1
	
runandclean: run clean
