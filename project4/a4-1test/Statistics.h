#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include <map>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

using namespace std;

/* Data structure to store table information and attributes*/
class TableInfo
{
private:
	long numTuples;
	map<string, int> tableAtts;
	string relName;
	int relSize;

public:
	TableInfo(long numTuples, string relName);
	TableInfo(TableInfo &tableInfo);
	~TableInfo();

	//getter methods
	map<string, int> *GetTableAtts();
	long GetNumTuples();
	string GetRelName();
	int GetRelSize();

	//Update container Data,Overloaded functions
	void SetNumTuples(int n);
	void SetAtt(string s, int num_distinct);
	void SetRelDetails(string groupName, int groupSize);
};

/*Container class for Database Statistics to store information on its attributes and relations*/
class Statistics
{
private:
	map<string, TableInfo *> stats;

public:
	Statistics();
	Statistics(Statistics &statsObj);
	~Statistics();

	map<string, TableInfo *> *GetStats();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	bool Validate(struct AndList *parseTree, char *relNames[], int numToJoin, map<string, long> &valuesList);
	bool ContainsAtt(char *value, char *relNames[], int numToJoin, map<string, long> &valuesList);
	double Evaluate(struct OrList *orList, map<string, long> &valuesList);
};
#endif