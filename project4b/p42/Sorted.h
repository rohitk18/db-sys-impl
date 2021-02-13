#ifndef SORTED_H
#define SORTED_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"
#include "Heap.h"

#define PIPE_BUFFER 100

typedef enum
{
    Read,
    Write
} Mode;

class Sorted : virtual public GenericDBFile
{

private:
    // Page *outpipeWritePage;
    char *fpath;
    File *sortedFile; //sortedfile
    File *heapFile;
    Page *wPage;     //Record writes go into this page
    Page *rPage;     //This page is only for reading
    int currentPage; //Current Page being read. 0 means no pages to read
    bool dirty;      //If true, current page being read is dirty(Not yet written to disk).

    Mode mode;

    Pipe *in;
    Pipe *out;
    OrderMaker *sortorder;
    int runlen;
    Heap *heap;
    BigQArgs *bigqargs;
    pthread_t thread;

public:
    static int addcount;

    //Constructor
    Sorted();
    //Destructor
    ~Sorted();

    int Create(char *fpath, fType file_type, void *startup);
    int Open(char *fpath);
    int Close();

    void Load(Schema &myschema, char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

    int GetFromMetaData(ifstream &ifs);
    int GetPage(Page *putItHere, off_t whichPage);
    void SwitchMode();
    void Merge();
};

void *producer(void *arg);

#endif // !SORTED_H