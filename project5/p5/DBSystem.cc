#include <iostream>

#include "DBSystem.h"
#include "Statistics.h"
#include "QueryPlanner.h"
#include "DDL.h"

typedef struct yy_buffer_state *YY_BUFFER_STATE;
extern "C" int yyparse(void); // defined in y.tab.c
extern "C" YY_BUFFER_STATE yy_scan_string(char *str);
extern "C" void yy_delete_buffer(YY_BUFFER_STATE buffer);

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

using namespace std;

void DBSystem::run()
{
    char *fileName = "Statistics.txt";
    Statistics s;
    DDL d;
    QueryPlanner qp(&s);

    string stir;
    char *cstir;
    while (true)
    {
    cout << " Enter CNF predicate (when done press Enter):\n\t";
    getline(cin, stir);
    if(stir.compare("QUIT")==0)
    {
        cout << "Quitting!" << endl;
        return;
    }
    cstir = &stir[0];
    YY_BUFFER_STATE buf =  yy_scan_string(cstir);

    if (yyparse() != 0)
    {
        cout << "Can't parse your SQL.\n";
        yy_delete_buffer(buf);
        exit(1);
    }
    yy_delete_buffer(buf);

    s.Read(fileName);

    cout << endl;

    if (newTable)
    {
        if (d.createNewTable())
            cout << "Create table " << newTable << endl;
        else
            cout << "Table " << newTable << " already exists." << endl;
        }
        else if (oldTable && newFile)
        {
            if (d.insertIntoTable())
                cout << "Insert into " << oldTable << endl;
            else
                cout << "Insert failed." << endl;
        }
        else if (oldTable && !newFile)
        {
            if (d.dropTable())
                cout << "Drop table " << oldTable << endl;
            else
                cout << "Table " << oldTable << " does not exist." << endl;
        }
        else if (newOutput)
        {
            qp.setOutput(newOutput);
        }
        if (attsToSelect || finalFunction)
        {
            qp.plan();
            qp.print();
            qp.run();
        }
        clear();
        //     i++;
    }
}

void DBSystem::clear()
{
    newAttrs = NULL;
    finalFunction = NULL;
    tables = NULL;
    boolean = NULL;
    groupingAtts = NULL;
    attsToSelect = NULL;
    newTable = oldTable = newFile = newOutput = NULL;
    distinctAtts = distinctFunc = 0;
    // FATALIF(!remove("*.tmp"), "remove tmp files failed");
}