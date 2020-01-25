CC = gcc
CFLAGS  = -g -Wall 

launch: test_assign1_1.o storage_mgr.o dberror.o 
	$(CC) $(CFLAGS) -o launch test_assign1_1.o storage_mgr.o dberror.o -lm

clean: 
	$(RM) launch *.o *~

run: launch
	./launch
	
randclean: run clean
