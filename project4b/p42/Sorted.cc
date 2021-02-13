#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Sorted.h"
#include "Defs.h"
#include <sstream>
#include "DBFile.h"
#include <unistd.h>

ComparisonEngine comparisonEng;
int Sorted::addcount = 0;

Sorted::Sorted()
{
    currentPage = 0;
    sortedFile = new File();
    rPage = new Page();
    heapFile = new File();
    wPage = new Page();
    sortorder = new OrderMaker();
    heap = new Heap();
}

Sorted::~Sorted()
{
    delete heapFile;
    delete rPage;
    delete wPage;
    delete sortedFile;
}

int Sorted::GetFromMetaData(ifstream &ifs)
{
    string line;
    if (ifs.is_open())
    {
        getline(ifs, line); // sorted type
        getline(ifs, line); // runlen
        std::stringstream s_str(line);
        s_str >> runlen;
        sortorder->GetFromFile(ifs);
    }
}

/* 
  Create metadata file
 */
int Sorted::Create(char *f_path, fType f_type, void *startup)
{
    mode = Read;

    SortInfo *sortinfo;
    char meta_path[100];
    sprintf(meta_path, "%s.meta", f_path);
    ofstream ofs(meta_path);
    if (!ofs)
    {
        cout << "Couldn't open file." << endl;
    }
    ofs << "sorted" << endl;
    sortinfo = (SortInfo *)startup;
    runlen = sortinfo->runlen;
    *sortorder = *(sortinfo->sortorder);

    ofs << runlen << endl;
    sortorder->FilePrint(ofs);

    heapFile->Open(0, f_path);
    heap->Create(f_path, fType::heap, NULL);

    return 1;
}

/*
    Changes mode to write and adds sorted records to pipe
 */
void Sorted::Add(Record &rec)
{

    if (mode == Read)
    {
        SwitchMode();
    }
    in->Insert(&rec);
}

/*
    Loads records from table file
*/
void Sorted::Load(Schema &f_schema, char *loadpath)
{
    Record temp;

    FILE *tableFile = fopen(loadpath, "r");

    if (tableFile)
    {
        while (temp.SuckNextRecord(&f_schema, tableFile) == 1)
        {
            Add(temp);
        }
    }
}

int Sorted::Open(char *f_path)
{
    char meta_path[100];
    sprintf(meta_path, "%s.meta", f_path);

    ifstream ifs(meta_path);
    if (!ifs)
    {
        cout << "Couldn't open file." << endl;
    }

    mode = Read;

    GetFromMetaData(ifs);

    heapFile->Open(1, f_path);
    heap->Open(f_path);
}

/*
    target to first position of binary file
*/
void Sorted::MoveFirst()
{
    if (mode == Write)
    {
        SwitchMode();
    }
    else if (mode == Read)
    {
        heap->MoveFirst();
        currentPage = 0;
    }
}

/*
    closes sorted binary file
*/
int Sorted::Close()
{

    if (mode == Write)
    {
        SwitchMode();
        return heap->Close();
    }
    else if (mode == Read)
    {
        return (heapFile->Close() < 0) ? 0 : 1;
    }
}

/*
    fetches next record from binary file
 */
int Sorted::GetNext(Record &fetchme)
{
    if (mode == Write)
    {
        SwitchMode();
    }
    else if (mode == Read)
    {
        return heap->GetNext(fetchme);
    }
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    int compare;
    ComparisonEngine ce;
    do
    {
        if (GetNext(fetchme) == 1)
            compare = ce.Compare(&fetchme, &literal, &cnf);
        else
            return 0;
    } while (compare == 0);
    return compare;
}

int Sorted::GetPage(Page *putItHere, off_t whichPage)
{
    if (whichPage < heapFile->GetLength() - 1)
    {
        return heapFile->GetPage(putItHere, whichPage);
    }
    else if (whichPage <= heapFile->GetLength())
    {
        heapFile->AddPage(wPage, currentPage);
        wPage->EmptyItOut();
        return heapFile->GetPage(putItHere, whichPage);
    }
    return 0;
}

void *run_bigq(void *args)
{

    BigQArgs *t = (BigQArgs *)args;
    BigQ bigq(*(t->in), *(t->out), *(t->sortorder), t->runlen);
}

void Sorted::Merge()
{

    bool fileEmpty = heap->isEmpty();
    off_t whichPage = 0;
    Record fileRecord;
    Record pipeRecord;

    if (!fileEmpty)
    {

        Pipe *inTemp;
        Pipe *outTemp;
        BigQArgs *argsTemp;
        pthread_t tempthread;
        inTemp = new (std::nothrow) Pipe(PIPE_BUFFER);
        outTemp = new (std::nothrow) Pipe(PIPE_BUFFER);
        argsTemp = new BigQArgs();
        argsTemp->in = inTemp;
        argsTemp->out = outTemp;
        argsTemp->sortorder = sortorder;
        argsTemp->runlen = runlen;

        int numRecordsOutPipe = 0;
        Heap *tempFile = new Heap();
        char *a = "tmp";
        tempFile->Create(a, fType::heap, NULL);
        while (out->Remove(&pipeRecord))
        {
            heap->Add(pipeRecord);
            numRecordsOutPipe++;
        }
        heap->FlushWritePage();

        pthread_join(thread, NULL);

        pthread_create(&tempthread, NULL, run_bigq, (void *)argsTemp);

        int numFileRecords = 0;
        heap->MoveFirst();
        while (heap->GetNext(fileRecord))
        {
            inTemp->Insert(&fileRecord);
            numFileRecords++;
        }
        inTemp->ShutDown();
        Record sortedRecord;
        numFileRecords = 0;

        while (outTemp->Remove(&sortedRecord))
        {
            tempFile->Add(sortedRecord);
            numFileRecords++;
        }
        tempFile->FlushWritePage();

        delete heap;
        rename(a, fpath);
        heap = tempFile;
    }
    else
    {
        int numRecords = 0;
        while (out->Remove(&pipeRecord))
        {
            heap->Add(pipeRecord);
            numRecords++;
        }
        heap->FlushWritePage();
    }
}

/*
    Changes read<->write mode
*/
void Sorted::SwitchMode()
{
    if (mode == Read)
    {
        mode = Write;
        in = new (std::nothrow) Pipe(PIPE_BUFFER);
        out = new (std::nothrow) Pipe(PIPE_BUFFER);
        bigqargs = new BigQArgs();
        bigqargs->in = in;
        bigqargs->out = out;
        bigqargs->sortorder = sortorder;
        bigqargs->runlen = runlen;
        pthread_create(&thread, NULL, run_bigq, (void *)bigqargs);
    }
    else if (mode == Write)
    {
        mode = Read;
        in->ShutDown();
        Merge();
        delete bigqargs;
        delete in;
        delete out;
        in = NULL;
        out = NULL;
    }
}