#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
#include <vector>
#include <queue>

using namespace std;

class BigQ
{

	//DataStructures
	//Record temp to read in records from pipes

	//Record temp;
	vector<DBFile *> *runs;
	int numRuns;
	pthread_t worker;
	int page_Index;
	// char *filename;

	struct SortArgs
	{
		Pipe *input;
		Pipe *output;
		OrderMaker *sortorder;
		int *runlen;
	} sortArgs;

	typedef struct SortArgs SortArgs;

	static void *RunGenerate(void *arg);
	void *RunMerge(void *arg);
	static void Quicksort(vector<Record> &recordBuffer, int left, int right, OrderMaker &sortorder);
	static void SortRuns(Page *, int, File &, int &, OrderMaker *);

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();
};

typedef struct RWrap
{
	Record record;
	int runNum;
} rWrap;

class SortClass
{
private:
	OrderMaker *sortorder;

public:
	SortClass(OrderMaker *order)
	{
		this->sortorder = order;
	}
	SortClass(){};

	bool operator()(Record *f, Record *s) const
	{
		ComparisonEngine *ce;
		if (ce->Compare(f, s, this->sortorder) < 0)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	bool operator()(RWrap *f, RWrap *s) const
	{

		ComparisonEngine *ce;

		if (ce->Compare(&(f->rec), &(s->rec), this->sortorder) < 0)
		{
			//return true;
			return false;
		}

		else
		{
			//return false;
			return true;
		}
	}
};

#endif