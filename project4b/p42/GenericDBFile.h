#ifndef GENERICFILE_H
#define GENERICFILE_H

#include "Schema.h"
#include "Record.h"

struct SortInfo
{
    OrderMaker *sortorder;
    int runlen;
};

enum fType
{
    heap,
    sorted,
    tree
};
// typedef enum fType fType;

class GenericDBFile
{

public:
    GenericDBFile();

    virtual int Create(char *fpath, fType file_type, void *startup) = 0;
    virtual int Open(char *fpath) = 0;
    virtual int Close() = 0;

    virtual void Load(Schema &myschema, char *loadpath) = 0;

    virtual void MoveFirst() = 0;
    virtual void Add(Record &addme) = 0;
    virtual int GetNext(Record &fetchme) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
    // virtual int AddAtPage(Page *wrtpage) = 0;

    virtual ~GenericDBFile();
};

#endif // !GENERICFILE_H