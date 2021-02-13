#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Heap.h"

#include <iostream>
using namespace std;

Heap::Heap()
{
    current = new Record();
}

Heap::~Heap()
{
    delete current;
}

/*
    Creates a new empty binary database file
*/
int Heap::Create(char *f_path, fType f_type, void *startup)
{
    if (f_path == NULL)
        return 0;
    file.Open(0, (char *)f_path);
    this->filePath = (char *)f_path;
    poff = 0;
    roff = 0;
    pDirty = false;
    end = true;
    return 1;
}

/*
    Loads all the records from table files to respective binary database files
*/
void Heap::Load(Schema &f_schema, char *loadpath)
{
    cout << "Loading all the records from tbl file..." << endl;
    FILE *tblFile = fopen(loadpath, "r");
    Record record;
    while (record.SuckNextRecord(&f_schema, tblFile) != 0)
    {
        Add(record);
    }
    fclose(tblFile);
    if (pDirty)
    {
        file.AddPage(&page, poff++);
        page.EmptyItOut();
        end = true;
        pDirty = false;
    }
    cout << "All records loaded." << endl;
}

/*
    Opens the already existing database binary files
*/
int Heap::Open(char *f_path)
{
    if (f_path == NULL)
        return 0;
    file.Open(1, (char *)f_path);
    return 1;
}

/*
    Brings the current pointer to start of binary file and first page of file is loaded
*/
void Heap::MoveFirst()
{
    file.GetPage(&page, 0);
    roff = 0;
    end = false;
    page.GetFirst(current);
}

/*
    Closes the opened binary files
*/
int Heap::Close()
{
    if (pDirty)
    {
        file.AddPage(&page, poff++);
        page.EmptyItOut();
        end = true;
        pDirty = false;
    }
    cout << "Closing file." << endl;
    return file.Close();
}

/*
    Adds(appends) one record to the binary file
*/
void Heap::Add(Record &rec)
{
    if (&rec == NULL)
        return;

    pDirty = true;
    if (page.Append(&rec) == 0)
    {
        file.AddPage(&page, poff++);
        page.EmptyItOut();
        page.Append(&rec);
    }
}

/*
    Fetches the next record in the page
*/
int Heap::GetNext(Record &fetchme)
{
    if (!end)
    {
        fetchme.Copy(current);
        if (page.GetFirst(current) == 0)
        {
            if (++roff >= file.GetLength() - 1)
                end = true;
            else
            {
                file.GetPage(&page, roff);
                page.GetFirst(current);
            }
        }
        return 1;
    }
    return 0;
}

/*
    Fetches the next record which statisfies the inputted CNF statement 
*/
int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    ComparisonEngine comp;
    while (this->GetNext(fetchme) == 1)
    {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

void Heap::FlushWritePage()
{
    int currlen = file.GetLength();
    int whichpage = currlen == 0 ? 0 : currlen - 1;
    file.AddPage(&wpage, whichpage);
}

bool Heap::isEmpty()
{
    return (file.GetLength() == 0 && wpage.GetNumRecs() == 0 && page.GetNumRecs() == 0);
}