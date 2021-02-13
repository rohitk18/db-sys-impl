#include "string.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

DBFile::DBFile()
{
	gdb = NULL;
}

DBFile::~DBFile()
{
	delete gdb;
	gdb = NULL;
}

int DBFile::Create(char *f_path, fType f_type, void *startup)
{
	char meta_path[100];
	sprintf(meta_path, "%s.meta", f_path);
	ofstream ofs(meta_path);
	if (!ofs)
	{
		cout << "Couldn't open file." << endl;
	}
	if (f_type == heap)
	{
		gdb = new Heap();
		ofs << "heap\n";
	}
	else if (f_type = sorted)
	{
		gdb = new Sorted();
	}
	return gdb->Create(f_path, f_type, startup);
}

void DBFile::Load(Schema &f_schema, char *loadpath)
{

	gdb->Load(f_schema, loadpath);
}

int DBFile::Open(char *f_path)
{
	char meta_path[100];
	string line;
	sprintf(meta_path, "%s.meta", f_path);
	ifstream ifs(meta_path);
	if (!ifs)
	{
		cout << "Couldn't open metadata file for reading" << endl;
		return 1;
	}
	if (ifs.is_open())
	{
		if (getline(ifs, line))
		{
			if (line.compare("heap") == 0)
			{
				gdb = new Heap();
			}
			else if (line.compare("sorted") == 0)
			{
				gdb = new Sorted();
			}
		}
	}
	return gdb->Open(f_path);
}

void DBFile::MoveFirst()
{
	gdb->MoveFirst();
}

int DBFile::Close()
{

	return gdb->Close();
}

void DBFile::Add(Record &rec)
{

	gdb->Add(rec);
}

int DBFile::GetNext(Record &fetchme)
{

	return gdb->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{

	return gdb->GetNext(fetchme, cnf, literal);
}

// int DBFile::AddAtPage(Page *wrtpage)
// {
// 	return gdb->AddAtPage(wrtpage);
// }