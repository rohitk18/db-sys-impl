#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

#include <iostream>
// #include <sys/stat.h>
using namespace std;

DBFile::DBFile()
{
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    file.Close();
    // struct stat exist;
    file.Open(0, (char *)f_path);
    this->filePath = (char *)f_path;
    // cout << file.GetLength() << " file's length" << endl;
    poff = 0;
    roff = 0;
    pDirty = false;

    // return stat(f_path, &exist) + 1;
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
    FILE *tblFile = fopen(loadpath, "r");
    Record record;
    // cout << record.SuckNextRecord(&f_schema, tblFile) << endl;
    // record.Print(&f_schema);
    while (record.SuckNextRecord(&f_schema, tblFile) != 0)
    {
        // cout << "add " << file.GetLength() << endl;
        // record.Print(&f_schema);
        // cout << endl;
        Add(record);
    }
    // cout << file.GetLength() << endl;
    fclose(tblFile);
    if (pDirty)
    {
        // cout << poff << endl;
        file.AddPage(&page, poff);
        // cout << file.GetLength() << " file's length" << endl;
        page.EmptyItOut();
        pDirty = false;
    }
    file.GetPage(&page, 0);
}

int DBFile::Open(const char *f_path)
{
    file.Close();
    file.Open(1, (char *)f_path);

    return 1;
}

void DBFile::MoveFirst()
{
    file.GetPage(&page, 0);
    roff = 0;
    page.GetFirst(current);
}

int DBFile::Close()
{
    // cout << file.GetLength() << " file's length" << endl;
    // cout << file.Close() << " close method" << endl;
    return 1;
}

void DBFile::Add(Record &rec)
{
    if (&rec == NULL)
    {
        return;
    }

    if (file.GetLength() > 1)
    {
        poff = file.GetLength() - 2;
        page.EmptyItOut();
        file.GetPage(&page, poff);
    }

    pDirty = true;
    if (page.Append(&rec) == 0)
    {
        file.AddPage(&page, poff++);
        // cout << file.GetLength() << " file's length" << endl;
        // cout << "New page " << poff << endl;
        page.EmptyItOut();
        page.Append(&rec);
    }
}

int DBFile::GetNext(Record &fetchme)
{
    if (current == NULL)
        return 0;
    fetchme.Consume(current);
    if (page.GetFirst(current) == 0)
    {
        // write if with roff and get length comparison
        if (++roff < file.GetLength())
        {
            file.GetPage(&page, ++roff);
            page.GetFirst(current);
        }
        else
        {
            current = NULL;
        }
    }
    // cout << &fetchme << endl;
    return 1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    ComparisonEngine comp;
    if (current == NULL)
        return 0;
    Record unmatch;
    while (current != NULL && !comp.Compare(current, &literal, &cnf))
    {
        unmatch.Consume(current);
        if (++roff < file.GetLength())
        {
            file.GetPage(&page, ++roff);
            page.GetFirst(current);
        }
        else
        {
            current = NULL;
            return 0;
        }
    }
    if (current != NULL)
    {
        fetchme.Consume(current);
        if (page.GetFirst(current) == 0)
        {
            if (++roff < file.GetLength())
            {
                file.GetPage(&page, ++roff);
                page.GetFirst(current);
            }
            else
            {
                current = NULL;
            }
        }
    }

    // cout << &fetchme << endl;
    return 1;
}
