#include <iostream>
#include <string>
#include "ParseTree.h"
#include "QueryPlanner.h"

using namespace std;

char *catalog_path = "catalog";
char *dbfile_dir = "";
char *tpch_dir = "";

extern "C"
{
	int yyparse(void); // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables;		   // the list of tables and aliases in the query
extern struct AndList *boolean;			   // the predicate in the WHERE clause
extern struct NameList *groupingAtts;	   // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect;	   // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts;				   // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;				   // 1 if there is a DISTINCT in an aggregate query

int main(int argc, char *argv[])
{
	int ret = yyparse();
	if (ret != 0 )
	{
		cout << "Can't parse SQL.\n";
		exit(1);
	}
	char *fileName = "Statistics.txt";
	Statistics s;
	s.Read(fileName);

	QueryPlanner plan(&s);
	return 0;
}