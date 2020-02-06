# 123124324

Compiler = gcc
CFlags  = -g -Wall
 
default: test2_1

test2_1: test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(Compiler) $(CFlags) -o test1 test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

test2_2: test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(Compiler) $(CFlags) -o test2 test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

test_assign2_1.o: test_assign2_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(Compiler) $(CFlags) -c test_assign2_1.c -lm

test_assign2_2.o: test_assign2_2.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(Compiler) $(CFlags) -c test_assign2_2.c -lm

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(Compiler) $(CFlags) -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(Compiler) $(CFlags) -c buffer_mgr.c

storage_mgr.o: storage_mgr.c storage_mgr.h 
	$(Compiler) $(CFlags) -c storage_mgr.c -lm

dberror.o: dberror.c dberror.h 
	$(Compiler) $(CFlags) -c dberror.c

clean: 
	$(RM) test2_1 test2_2 *.o *~

run_test2_1:
	./test2_1

run_test2_2:
	./test2_2
