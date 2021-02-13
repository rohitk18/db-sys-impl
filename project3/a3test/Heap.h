#ifndef HEAP_H
#define HEAP_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

class Heap : public GenericDBFile
{
private:
    File file;
    char *filePath;
    Record *current;
    Page page;
    Page *write_page;
    off_t poff;  // int value of number of completed pages
    off_t roff;  // reading page value
    bool pDirty; // true on page changes else false
    bool end;    // true on file end else false

public:
    Heap();
    ~Heap();

    int Create(char *fpath, fType file_type, void *startup);
    int Open(char *fpath);
    int Close();

    void Load(Schema &myschema, char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    bool isEmpty();
    void FlushWritePage();
};

#endif // !HEAP_H