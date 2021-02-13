#include "BigQ.h"
#include <stdlib.h>

using namespace std;

class SortRun
{
	OrderMaker *sortorder;
public:
	SortRun(OrderMaker *so)
	{
		sortorder = so;
	}
	bool operator()(Record *r1, Record *r2)
	{
		ComparisonEngine ce;
		if (ce.Compare(r1, r2, sortorder) < 0)
			return true;
		else
			return false;
	}
};

class SortMerge
{
	OrderMaker *sortorder;
public:
	SortMerge(OrderMaker *so)
	{
		sortorder = so;
	}
	bool operator()(Record *r1, Record *r2)
	{
		ComparisonEngine ce;
		if (ce.Compare(r1, r2, sortorder) < 0)
			return false;
		else
			return true;
	}
};

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{

	if (runlen <= 0)
	{
		out.ShutDown();
		return;
	}
	map<int, Page *> overflow;
	int count = 0;
	int pageCount = 0;
	Record record;
	Record *tempRecord;
	File file;
	Page *page = new (std::nothrow) Page();
	Page *pageToFile;
	vector<Record *> recordVector;
	vector<Page *> pageVector;
	char tempFile[100];
	sprintf(tempFile, "temp%d.bin", rand());
	file.Open(0, tempFile); // tempfile for maintaining sorted run
	while (true)
	{

		if (in.Remove(&record) && count < runlen)
		{
			if (!page->Append(&record))
			{
				pageVector.push_back(page);
				page = new (std::nothrow) Page();
				page->Append(&record);
				count++;
			}
		}

		else
		{ 
			//Got runlen pages, now sort them

			//Incase of last run, push the large page for sorting
			if (count < runlen && page->GetNumRecs())
			{
				pageVector.push_back(page);
			}

			//Extract records of each run from pages, do an in-memory sort
			for (int i = 0; i < pageVector.size(); i++)
			{
				tempRecord = new (std::nothrow) Record();
				while (pageVector[i]->GetFirst(tempRecord))
				{
					recordVector.push_back(tempRecord);
					tempRecord = new (std::nothrow) Record();
				}
				delete tempRecord;
				delete pageVector[i];
				pageVector[i] = NULL;
				tempRecord = NULL;
			}

			//Using in-built algorithm for sort
			std::sort(recordVector.begin(), recordVector.end(), SortRun(&sortorder));

			//Writing each run into temp file
			int tempPageCount = 0;
			pageToFile = new (std::nothrow) Page();
			for (int i = 0; i < recordVector.size(); i++)
			{
				if (!pageToFile->Append(recordVector[i]))
				{
					tempPageCount++;
					file.AddPage(pageToFile, pageCount++);
					pageToFile->EmptyItOut();
					pageToFile->Append(recordVector[i]);
				}
				delete recordVector[i];
				recordVector[i] = NULL;
			}

			if (tempPageCount < runlen)
			{
				if (pageToFile->GetNumRecs())
					file.AddPage(pageToFile, pageCount++);
			}
			else
			{ //Check for overflow page and map the run number to it
				if (pageToFile->GetNumRecs())
				{

					overflow[pageCount - 1] = pageToFile;
				}
			}
			pageToFile = NULL;

			recordVector.clear();
			pageVector.clear();

			//Appending the record of next-run into buffer page, and setting run-page count to 0
			if (count >= runlen)
			{
				page->Append(&record);
				count = 0;
				continue;
			}
			else
			{
				break;
			}
		}
	}

	//Parameters for merge step
	off_t fileLength = file.GetLength();
	off_t numRuns;
	if (fileLength != 0)
		numRuns = ((ceil)((float)(fileLength - 1) / runlen));
	else
		numRuns = 0;
	off_t lastRunPages = (fileLength - 1) - (numRuns - 1) * runlen;

	priority_queue<Record *, vector<Record *>, SortMerge> pQue(&sortorder);

	//Maps Record to run (for k-way merge)
	map<Record *, int> recordMap;

	//Maintains page numbers of each run
	int *pageIndex = new (std::nothrow) int[numRuns];

	//Maintains current buffer page in each run
	Page **pageArray = new (std::nothrow) Page *[numRuns];

	//Load first pages into the memory buffer and first record into priority queue
	int numPages = 0;

	for (int i = 0; i < numRuns; i++)
	{
		pageArray[i] = new Page();
		file.GetPage(pageArray[i], numPages);
		pageIndex[i] = 1;
		Record *r = new (std::nothrow) Record;
		pageArray[i]->GetFirst(r);

		pQue.push(r);
		recordMap[r] = i;
		r = NULL;
		numPages += runlen;
	}

	//Extract from priority queue and place on the output pipe
	while (!pQue.empty())
	{

		Record *r = pQue.top(); //Extract the min priority record
		pQue.pop();

		// run number of record being pushed, -1
		int nextRun = -1;

		nextRun = recordMap[r];
		recordMap.erase(r);

		if (nextRun == -1)
		{
			cout << "Since element which was mapped before isn't present" << endl;
			return;
		}

		Record *next = new (std::nothrow) Record;
		bool recordFound = true;
		if (!pageArray[nextRun]->GetFirst(next))
		{
			//Check if not the end of run
			if ((!(nextRun == (numRuns - 1)) && pageIndex[nextRun] < runlen) || ((nextRun == (numRuns - 1) && pageIndex[nextRun] < lastRunPages)))
			{

				file.GetPage(pageArray[nextRun], pageIndex[nextRun] + nextRun * runlen);
				pageArray[nextRun]->GetFirst(next);
				pageIndex[nextRun]++;
			}
			else
			{ //Check for overflow page incase of last run
				if (pageIndex[nextRun] == runlen)
				{
					if (overflow[(nextRun + 1) * runlen - 1])
					{
						delete pageArray[nextRun];
						pageArray[nextRun] = NULL;
						pageArray[nextRun] = (overflow[(nextRun + 1) * runlen - 1]);
						overflow[(nextRun + 1) * runlen - 1] = NULL;
						pageArray[nextRun]->GetFirst(next);
					}
					else
						recordFound = false;
				}
				else
					recordFound = false;
			}
		}

		//Push the next record into the priority queue if found
		if (recordFound)
		{
			pQue.push(next);
		}
		recordMap[next] = nextRun;
		//rec_head[nextRun]=next;

		out.Insert(r); //Put the min priority record onto the pipe
					   //cout<<temp_file<<endl;
	}

	file.Close(); // tempfile close
	remove(tempFile); // deleting the temp file
	out.ShutDown();
}

BigQ::~BigQ() {}