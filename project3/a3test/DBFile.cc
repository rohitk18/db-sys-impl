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
	sortorder = NULL;
	srunlen = 0;
}

DBFile::~DBFile()
{
	sortorder = NULL;
	delete gdb;
	gdb = NULL;
}

int DBFile::Create(char *f_path, fType f_type, void *startup)
{
	char metafile[100];
	strcpy(metafile, f_path);
	strcat(metafile, ".meta");

	ofstream myfile(metafile);

	if (f_type == heap)
	{
		if (myfile.is_open())
		{
			myfile << "heap\n";
			myfile.close();
		}
		else
		{
			return -1;
		}
		gdb = new Heap();
	}
	else if (f_type == sorted)
	{
		srunlen = ((SortInfo *)startup)->runLength;
		sortorder = ((SortInfo *)startup)->myOrder;
		if (myfile.is_open())
		{
			myfile << "sorted\n";
			myfile << srunlen << "\n";
			sortorder->PrintToFile(myfile); // serialize the OrderMaker Object
			myfile.close();
		}
		else
		{
			return -1;
		}
		gdb = new Sorted();
	}
	if (!gdb->Create(f_path, f_type, startup))
	{
		return -1;
	}
	/*if(!Open (f_path))
    {
        return -1;
    }*/
	return 1;
}

void DBFile::Load(Schema &f_schema, char *loadpath)
{

	//cout<<"reached in load of dbfile" << endl;
	gdb->Load(f_schema, loadpath);
}

int DBFile::Open(char *f_path)
{
	if (gdb == NULL) //1-Heap;2-Sorted Heap;3-Btree;
	{
		char metafile[100];
		strcpy(metafile, f_path);
		strcat(metafile, ".meta");
		string status;
		ifstream myfile(metafile);
		if (myfile.is_open())
		{
			getline(myfile, status);
		}
		else
		{
			return -1;
		}
		if (status.compare("heap") == 0)
		{
			gdb = new Heap();
		}
		else if (status.compare("sorted") == 0)
		{
			sortorder = new OrderMaker();
			srunlen = sortorder->CreateFromFile(myfile);
			gdb = new Sorted();
		}
		myfile.close();
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