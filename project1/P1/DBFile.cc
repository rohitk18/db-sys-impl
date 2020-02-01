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
    // pid = 1;
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
        Add(record);
    }
    cout << file.GetLength() << endl;
    fclose(tblFile);
}

int DBFile::Open(const char *f_path)
{
    file.Close();
    file.Open(1, (char *)f_path);

    return 1;
}

void DBFile::MoveFirst()
{
    file.GetPage(&page, 1);
    page.GetFirst(current);
}

int DBFile::Close()
{
    if(pDirty)
    {
        file.AddPage(&page,pid);
    }
    return file.Close();
}

void DBFile::Add(Record &rec)
{
    pDirty = true;
    if (page.Append(&rec) == 0)
    {
        file.AddPage(&page, pid);
        pid++;
        page.EmptyItOut();
        page.Append(&rec);
    }
}

int DBFile::GetNext(Record &fetchme)
{
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
}
