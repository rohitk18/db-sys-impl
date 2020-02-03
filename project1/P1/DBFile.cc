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
using namespace std;

DBFile::DBFile()
{
    current = new Record();
}

DBFile::~DBFile()
{
    delete current;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    file.Open(0, (char *)f_path);
    this->filePath = (char *)f_path;
    poff = 0;
    roff = 0;
    pDirty = false;
    end = true;
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
    FILE *tblFile = fopen(loadpath, "r");
    Record record;
    while (record.SuckNextRecord(&f_schema, tblFile) != 0)
    {
        Add(record);
    }
    cout << file.GetLength() << " line 48" << endl;
    fclose(tblFile);
    if (pDirty)
    {
        cout << poff << " line 52" << endl;
        file.AddPage(&page, poff++);
        cout << file.GetLength() << " file's length" << endl;
        page.EmptyItOut();
        end = true;
        pDirty = false;
    }
}

int DBFile::Open(const char *f_path)
{
    file.Open(1, (char *)f_path);
    current = new Record();

    return 1;
}

void DBFile::MoveFirst()
{
    file.GetPage(&page, 0);
    roff = 0;
    end = false;
    page.GetFirst(current);
}

int DBFile::Close()
{
    return file.Close();
}

void DBFile::Add(Record &rec)
{
    if (&rec == NULL)
        return;

    pDirty = true;
    if (page.Append(&rec) == 0)
    {
        file.AddPage(&page, poff++);
        // cout << file.GetLength() << " file's length" << endl;
        page.EmptyItOut();
        page.Append(&rec);
    }
}

int DBFile::GetNext(Record &fetchme)
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

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    ComparisonEngine comp;
    while (this->GetNext(fetchme) == 1)
    {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}
