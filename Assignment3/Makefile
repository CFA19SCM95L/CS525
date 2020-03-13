Compiler = gcc
file = buffer_mgr.o buffer_mgr_stat.o dberror.o expr.o rm_serializer.o storage_mgr.o record_mgr.o

test_expr : $(file) test_expr.o
	$(Compiler) -o test_expr $(file) test_expr.o
	rm *.o

test : $(base) test_assign3_1.o
	$(Compiler) -o test $(file) test_assign3_1.o
	rm *.o

buffer_mgr.o : buffer_mgr.c
	$(Compiler) -c buffer_mgr.c -I .

buffer_mgr_stat.o : buffer_mgr_stat.c
	$(Compiler) -c buffer_mgr_stat.c -I .

dberror.o : dberror.c
	$(Compiler) -c dberror.c -I .

expr.o : expr.c
	$(Compiler) -c expr.c -I .

rm_serializer.o : rm_serializer.c
	$(Compiler) -c rm_serializer.c -I .

storage_mgr.o : storage_mgr.c
	$(Compiler) -c storage_mgr.c -I .

test_expr.o : test_expr.c
	$(Compiler) -c test_expr.c -I .

test_assign3_1.o : test_assign3_1.c
	$(Compiler) -c test_assign3_1.c -I .

record_mgr.o : record_mgr.c
	$(Compiler) -c record_mgr.c -I .

.PHONY : clean
clean :
	rm test_expr test

defaultTest : buffer_mgr.o buffer_mgr_stat.o dberror.o expr.o rm_serializer.o storage_mgr.o test_expr.o test_assign3_1.o record_mgr.o test