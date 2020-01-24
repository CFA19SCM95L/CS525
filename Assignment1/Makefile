Compiler:= gcc
Source := dberror.c storage_mgr.c test_assign1_1.c
Object := dberror.o storage_mgr.o test_assign1_1.o

code_assignment1: $(Object)
	$(Compiler) -o test_assign1_1 $?

%.o: %.c
	$(Compiler) -g -c $<

run: code_assignment1
	./test_assign1_1

clean:
	rm -rf test_assign1_1 *.o
