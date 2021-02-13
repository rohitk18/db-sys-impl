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
	char *fileName = "Statistics.txt";
	Statistics s;
	char *relName[] = {
		"customer",
		"lineitem",
		"nation",
		"orders",
		"part",
		"partsupp",
		"region",
		"supplier"
	};

	s.AddRel(relName[0],150000);
	s.AddAtt(relName[0],"c_custkey",150000);
	s.AddAtt(relName[0],"c_name", 150000);
	s.AddAtt(relName[0],"c_address", 150000);
	s.AddAtt(relName[0],"c_nationkey", 25);
	s.AddAtt(relName[0],"c_phone", 150000);
	s.AddAtt(relName[0],"c_acctbal", 140187);
	s.AddAtt(relName[0],"c_mktsegment", 5);
	s.AddAtt(relName[0],"c_comment", 149968);

	s.AddRel(relName[1],6001215);
	s.AddAtt(relName[1],"l_orderkey",1500000);
	s.AddAtt(relName[1],"l_partkey",200000);
	s.AddAtt(relName[1],"l_suppkey", 10000);
	s.AddAtt(relName[1],"l_linenumber", 7);
	s.AddAtt(relName[1],"l_quantity", 50);
	s.AddAtt(relName[1],"l_extendedprice", 933900);
	s.AddAtt(relName[1],"l_discount", 11);
	s.AddAtt(relName[1],"l_tax", 9);
	s.AddAtt(relName[1],"l_returnflag", 3);
	s.AddAtt(relName[1],"l_linestatus", 2);
	s.AddAtt(relName[1],"l_shipdate", 2526);
	s.AddAtt(relName[1],"l_commitdate", 2466);
	s.AddAtt(relName[1],"l_receiptdate", 2554);
	s.AddAtt(relName[1],"l_shipinstruct", 4);
	s.AddAtt(relName[1],"l_shipmode", 7);
	s.AddAtt(relName[1],"l_comment", 4580667);

	s.AddRel(relName[2],25);
	s.AddAtt(relName[2],"n_nationkey", 25);
	s.AddAtt(relName[2],"n_name", 25);
	s.AddAtt(relName[2],"n_regionkey", 5);
	s.AddAtt(relName[2],"n_comment", 25);

	s.AddRel(relName[3],1500000);
	s.AddAtt(relName[3],"o_orderkey", 1500000);
	s.AddAtt(relName[3],"o_custkey", 99996);
	s.AddAtt(relName[3],"o_orderstatus", 3);
	s.AddAtt(relName[3],"o_totalprice", 1464556);
	s.AddAtt(relName[3],"o_orderdate", 2406);
	s.AddAtt(relName[3],"o_orderPriority", 5);
	s.AddAtt(relName[3],"o_clerk", 1000);
	s.AddAtt(relName[3],"o_shipPriority", 1);
	s.AddAtt(relName[3],"o_comment", 1482071);

	s.AddRel(relName[4],200000);
	s.AddAtt(relName[4],"p_partkey", 200000);
	s.AddAtt(relName[4],"p_name", 199996);
	s.AddAtt(relName[4],"p_mfgr", 5);
	s.AddAtt(relName[4],"p_brand", 25);
	s.AddAtt(relName[4],"p_type", 150);
	s.AddAtt(relName[4],"p_size", 50);
	s.AddAtt(relName[4],"p_container", 40);
	s.AddAtt(relName[4],"p_retailprice", 20899);
	s.AddAtt(relName[4],"p_comment", 131753);


	s.AddRel(relName[5],800000);
	s.AddAtt(relName[5],"ps_partkey", 200000);
	s.AddAtt(relName[5],"ps_suppkey", 10000);
	s.AddAtt(relName[5],"ps_availqty", 9999);
	s.AddAtt(relName[5],"ps_supplycost", 99865);
	s.AddAtt(relName[5],"ps_comment", 799124);

	s.AddRel(relName[6],5);
	s.AddAtt(relName[6],"r_name", 5);
	s.AddAtt(relName[6],"r_regionkey", 5);
	s.AddAtt(relName[6],"r_comment", 5);

	s.AddRel(relName[7],10000);
	s.AddAtt(relName[7],"s_suppkey", 10000);
	s.AddAtt(relName[7],"s_name", 10000);
	s.AddAtt(relName[7],"s_address", 10000);
	s.AddAtt(relName[7],"s_nationkey", 25);
	s.AddAtt(relName[7],"s_phone", 10000);
	s.AddAtt(relName[7],"s_acctbal", 9955);
	s.AddAtt(relName[7],"s_comment", 10000);
	
	s.Write(fileName);

	int ret = yyparse();
	if (ret != 0 )
	{
		cout << "Can't parse SQL.\n";
		exit(1);
	}

	s.Read(fileName);

	QueryPlanner plan(&s);
	plan.plan();
	plan.print();
	return 0;
}