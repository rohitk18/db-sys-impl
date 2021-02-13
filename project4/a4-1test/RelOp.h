#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp
{
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone() = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp
{

private:
	// pthread_t thread;
	// Record *buffer;
	pthread_t thread;
	Pipe *outPipe;
	DBFile *inFile;
	CNF *selOp;
	Record *literal;
	int nPages; // run length

public:
	void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	static void *operationHelper(void *args);
	void *operation();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class SelectPipe : public RelationalOp
{
private:
	pthread_t thread;
	Pipe *outPipe;
	Pipe *inPipe;
	CNF *selOp;
	Record *literal;
	int nPages; // run length
public:
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
class Project : public RelationalOp
{
private:
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	pthread_t thread;
	int nPages;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
class Join : public RelationalOp
{
private:
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	pthread_t thread;
	int nPages;

public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
typedef struct
{
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	int RunPages;
} join_util;
class DuplicateRemoval : public RelationalOp
{
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	int nPages;
	pthread_t thread;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
class Sum : public RelationalOp
{
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	int nPages;
	pthread_t thread;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
class GroupBy : public RelationalOp
{
private:
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
	int nPages;
	pthread_t thread;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
class WriteOut : public RelationalOp
{
private:
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;
	int nPages;
	pthread_t thread;

public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	void *operation();
	static void *operationHelper(void *args);
};
#endif
