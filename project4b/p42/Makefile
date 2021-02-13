CC = g++ -O2 -Wno-deprecated -std=c++11
LD_FLAGS = -lpthread -lgtest

test_out_tag = -ll

tag = -i

ifdef linux
tag = -n
endif

all: a42.out a4-1.out a2test.out gtest

a42.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o Heap.o Sorted.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o y.tab.o QueryPlanner.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o a42.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o Heap.o Sorted.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o y.tab.o QueryPlanner.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o Heap.o Sorted.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o y.tab.o lex.yy.o a41test.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o Heap.o Sorted.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o y.tab.o lex.yy.o a41test.o -lfl -lpthread

a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o Heap.o Sorted.o Pipe.o y.tab.o lex.yy.o a2test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o Heap.o Sorted.o Pipe.o y.tab.o lex.yy.o a2test.o -lfl -lpthread

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o Heap.o Sorted.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o QueryPlanner.o y.tab.o lex.yy.o utest.o
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o Heap.o Sorted.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o QueryPlanner.o y.tab.o lex.yy.o  utest.o -lfl $(LD_FLAGS)
	
test.o: test.cc
	$(CC) -g -c test.cc

a41test.o: a41test.cc
	$(CC) -g -c a41test.cc

a2test.o: a2test.cc
	$(CC) -g -c a2test.cc

utest.o: utest.cc
	$(CC) -g -c utest.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

QueryPlanner.o: QueryPlanner.cc
	$(CC) -g -c QueryPlanner.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	# sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	# sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f yyfunc.tab.*
	rm -f lex.yy.c
	rm -f lex.yyfunc*
	rm -f y.tab.h
