#include <stdio.h> // remove, move, etc
#include <cstring>
#include <fstream>

#include "DBFile.h"
#include "ParseTree.h"
#include "Comparison.h"
#include "DDL.h"

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables;           // the list of tables and aliases in the query
extern struct AndList *boolean;            // the predicate in the WHERE clause
extern struct NameList *groupingAtts;      // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect;      // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts;                   // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;                   // 1 if there is a DISTINCT in an aggregate query

extern char *newTable;
extern char *oldTable;
extern char *newFile;
extern char *newOutput;
extern struct AttrList *newAttrs;
extern struct NameList *sortAttrs;

extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

// create new table
bool DDL::createNewTable()
{
    if (exists(newTable))
        return false;
    ofstream ofMetaFile((string(newTable) + ".meta").c_str());
    fType type = (sortAttrs ? sorted : heap);
    ofMetaFile << type << endl; // defines file type of new table

    // define schema of new table
    int numAtts = 0;
    std::ofstream ofCatalog(catalog_path, std::ios_base::app);
    ofCatalog << "\n\nBEGIN\n"
              << newTable << '\n'
              << newTable << ".tbl" << endl;
    const char *myTypes[3] = {"Int", "Double", "String"};
    for (AttrList *att = newAttrs; att; att = att->next, ++numAtts)
        ofCatalog << att->name << ' ' << myTypes[att->type] << endl;
    ofCatalog << "END" << endl;

    Attribute *atts = new Attribute[numAtts];
    Type types[3] = {Int, Double, String};
    numAtts = 0;
    for (AttrList *att = newAttrs; att; att = att->next, numAtts++)
    {
        atts[numAtts].name = strdup(att->name);
        atts[numAtts].myType = types[att->type];
    }
    Schema newSchema("", numAtts, atts);

    // if sort exists
    OrderMaker sortOrder;
    if (sortAttrs)
    {
        sortOrder.growFromParseTree(sortAttrs, &newSchema);
        ofMetaFile << sortOrder;
        ofMetaFile << 512 << endl;
    }

    struct SortInfo
    {
        OrderMaker *myOrder;
        int runLength;
    } info = {&sortOrder, 512};
    DBFile newDBFTable;
    newDBFTable.Create((char *)(string(newTable) + ".bin").c_str(), type, (void *)&info); // create ".bin" files
    newDBFTable.Close();

    delete[] atts;
    ofMetaFile.close();
    ofCatalog.close();
    return true;
}

// insert record into existing table
bool DDL::insertIntoTable()
{
    DBFile table;
    char *fpath = new char[strlen(oldTable) + 4];
    strcpy(fpath, oldTable);
    strcat(fpath, ".bin");
    char *tblfile = newFile;
    strcat(tblfile, ".tbl");
    Schema schema(catalog_path, oldTable);
    if (table.Open(fpath))
    {
        table.Load(schema, tblfile);
        table.Close();
        delete[] fpath;
        return true;
    }
    delete[] fpath;
    return false;
}

// dropping table
bool DDL::dropTable()
{
    // delete from catalog
    string schemaStr = "";
    string line = "";
    string relName = oldTable;
    ifstream ifs(catalog_path);
    ofstream ofs(".catalog.tmp");
    bool found = false, exists = false;
    while (getline(ifs, line))
    {
        if (trim(line).empty())
            continue;
        if (line == oldTable)
            exists = true, found = true;
        schemaStr += trim(line) + '\n';
        if (line == "END")
        {
            if (!found)
                ofs << schemaStr << endl;
            found = false;
            schemaStr.clear();
        }
    }

    rename(".catalog.tmp", catalog_path);
    ifs.close();
    ofs.close();

    // delete bin, meta
    if (exists)
    {
        remove((relName + ".bin").c_str());
        remove((relName + ".meta").c_str());
        return true;
    }
    return false;
}

bool DDL::exists(const char *relName)
{
    ifstream ifs(catalog_path);
    string line;
    while (getline(ifs, line)){
        if (trim(line) == relName)
        {
            ifs.close();
            return true;
        }
    }
    ifs.close();
    return false;
}