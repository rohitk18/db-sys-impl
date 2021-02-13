#include "Statistics.h"

/* Methods for TableInfo Class*/
TableInfo::TableInfo(long numTuples, string relName) : numTuples(numTuples), relName(relName)
{
    relSize = 1;
}
/* Copy Constructor*/
TableInfo::TableInfo(TableInfo &tableInfo)
{
    // deep copy
    numTuples = tableInfo.GetNumTuples();
    map<string, int> *ptableAtt = tableInfo.GetTableAtts();
    map<string, int>::iterator iter;
    for (iter = ptableAtt->begin(); iter != ptableAtt->end(); iter++)
    {
        tableAtts[iter->first] = iter->second;
    }
    relSize = tableInfo.relSize;
    relName = tableInfo.relName;
}

TableInfo::~TableInfo()
{
    tableAtts.clear();
}

/* Setter methods */
void TableInfo::SetNumTuples(int num)
{
    numTuples = num;
}

void TableInfo::SetAtt(string s, int attValue)
{
    tableAtts[s] = attValue;
}

void TableInfo::SetRelDetails(string name, int size)
{
    relName = name;
    relSize = size;
}

/* getter methods */
map<string, int> *TableInfo::GetTableAtts()
{
    return &tableAtts;
}
long TableInfo::GetNumTuples()
{
    return numTuples;
}
string TableInfo::GetRelName()
{
    return relName;
}
int TableInfo::GetRelSize()
{
    return relSize;
}


/* Member methods for Statistics */
Statistics::Statistics()
{
}

/* Copy constructor */
Statistics::Statistics(Statistics &statsObj)
{
    map<string, TableInfo *> *p = statsObj.GetStats();
    map<string, TableInfo *>::iterator iter;
    TableInfo *tableInfo;
    //Iterate over the CopyMe HashMap and copy it over
    for (iter = p->begin(); iter != p->end(); iter++)
    {
        tableInfo = new TableInfo(*iter->second);
        stats[iter->first] = tableInfo;
    }
}

/* Destructor */
Statistics::~Statistics()
{
    map<string, TableInfo *>::iterator iter;
    TableInfo *tableInfo = NULL;
    //Iterate over the HashMap and delete the tablestat objects and then clear the HashMap
    for (iter = stats.begin(); iter != stats.end(); iter++)
    {
        tableInfo = iter->second;
        delete tableInfo;
        tableInfo = NULL;
    }
    stats.clear();
}

/* Gets map of table information of all tables in database*/
map<string, TableInfo *> *Statistics::GetStats()
{
    return &stats;
}

/* Adds relation information in statistics object */
void Statistics::AddRel(char *relName, int numTuples)
{
    /* if the stats contains the relation, update numTuples, otherwise add new entry */

    map<string, TableInfo *>::iterator iter;
    TableInfo *tableInfo;
    iter = stats.find(string(relName));
    if (iter != stats.end())
    {
        stats[string(relName)]->SetNumTuples(numTuples);
        stats[string(relName)]->SetRelDetails(relName, 1);
    }
    else
    {
        tableInfo = new TableInfo(numTuples, string(relName));
        stats[string(relName)] = tableInfo;
    }
}

/* Adds attributes to relation in statistics object */
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    map<string, TableInfo *>::iterator iter;
    iter = stats.find(string(relName));
    if (iter != stats.end())
    {
        if (numDistincts == -1)
            numDistincts = stats[string(relName)]->GetNumTuples();

        stats[string(relName)]->SetAtt(string(attName), numDistincts);
    }
}

/* if the statistics contains the relation, update the attribute in TableInfo, otherwise report error*/
void Statistics::CopyRel(char *oldName, char *newName)
{
    if (strcmp(oldName, newName) == 0)
        return;
    string oldRel = string(oldName);
    string newRel = string(newName);

    map<string, TableInfo *>::iterator newIter;
    newIter = stats.find(newRel);
    if (newIter != stats.end())
    {
        delete newIter->second;
        string temp = newIter->first;
        newIter++;
        stats.erase(temp);
    }

    map<string, TableInfo *>::iterator oldIter;

    oldIter = stats.find(oldRel);
    TableInfo *tp;

    if (oldIter != stats.end())
    {
        TableInfo *newTable = new TableInfo(stats[string(oldName)]->GetNumTuples(), newRel);
        tp = stats[oldRel];
        map<string, int>::iterator tableiter = tp->GetTableAtts()->begin();
        for (; tableiter != tp->GetTableAtts()->end(); tableiter++)
        {
            string temp = newRel + "." + tableiter->first;
            newTable->SetAtt(temp, tableiter->second);
        }
        stats[string(newName)] = newTable;
    }
    else
    {
        cout << "\n invalid relation name:" << oldName << endl;
        exit(1);
    }
}

/* Open file, fills statistics object, by scanning from BEGIN to END for each Relation */
void Statistics::Read(char *fromWhere)
{
    FILE *fptr = NULL;
    fptr = fopen(fromWhere, "r");
    char strRead[200];
    while (fptr != NULL && fscanf(fptr, "%s", strRead) != EOF)
    {
        if (strcmp(strRead, "BEGIN") == 0)
        {
            int tuplecnt = 0;
            char relname[200];
            int grpcnt = 0;
            char groupname[200];
            fscanf(fptr, "%s %ld %s %d", relname, &tuplecnt, groupname, &grpcnt);
            AddRel(relname, tuplecnt);
            stats[string(relname)]->SetAtt(groupname, grpcnt);
            char attname[200];
            int distcnt = 0;
            fscanf(fptr, "%s", attname);
            while (strcmp(attname, "END") != 0)
            {
                fscanf(fptr, "%d", &distcnt);
                AddAtt(relname, attname, distcnt);
                fscanf(fptr, "%s", attname);
            }
        }
    }
}

/* Iterate over the stats maps, for each entry(relation) iterate over attributes to print the numOfTuples, and numOfDistinctValues respectively */
void Statistics::Write(char *fromWhere)
{
    
    map<string, TableInfo *>::iterator dbitr;
    map<string, int>::iterator tbitr;
    map<string, int> *attrptr;

    FILE *fptr;
    fptr = fopen(fromWhere, "w");
    dbitr = stats.begin();

    for (; dbitr != stats.end(); dbitr++)
    {
        fprintf(fptr, "BEGIN\n");
        fprintf(fptr, "%s %ld %s %d\n", dbitr->first.c_str(), dbitr->second->GetNumTuples(), dbitr->second->GetRelName().c_str(), dbitr->second->GetRelSize());
        attrptr = dbitr->second->GetTableAtts();
        tbitr = attrptr->begin();

        for (; tbitr != attrptr->end(); tbitr++)
        {
            fprintf(fptr, "%s %d\n", tbitr->first.c_str(), tbitr->second);
        }
        fprintf(fptr, "END\n");
    }
    fclose(fptr);
}

/* Call Estimate , round the result and store in the statistics object */
void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{

    double r = Estimate(parseTree, relNames, numToJoin);

    long result = (long)((r - floor(r)) >= 0.5 ? ceil(r) : floor(r));

    string relName = "";
    int relSize = numToJoin;
    for (int i = 0; i < relSize; i++)
    {
        relName = relName + "," + relNames[i];
    }
    map<string, TableInfo *>::iterator itr = stats.begin();
    for (int i = 0; i < numToJoin; i++)
    {
        stats[relNames[i]]->SetRelDetails(relName, relSize);
        stats[relNames[i]]->SetNumTuples(result);
    }
}

/* Gives estimation of number of tuples*/
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double estimate = 1; // estimation of tuples
    map<string, long> valuesList;
    if (!Validate(parseTree, relNames, numToJoin, valuesList))
    {
        cout << "Input Parameters invalid for Estimation" << endl;
        return -1.0;
    }
    else
    {
        string relName = "";
        map<string, long>::iterator tupleIter;
        map<string, long> tupleVals;
        int relSize = numToJoin;
        for (int i = 0; i < relSize; i++)
        {
            relName = relName + "," + relNames[i];
        }
        for (int i = 0; i < numToJoin; i++)
        {
            tupleVals[stats[relNames[i]]->GetRelName()] = stats[relNames[i]]->GetNumTuples();
        }

        estimate = 1000.0; // Safety purpose so that we dont go out of Double precision

        while (parseTree != NULL)
        {
            estimate *= Evaluate(parseTree->left, valuesList);
            parseTree = parseTree->rightAnd;
        }
        tupleIter = tupleVals.begin();
        for (; tupleIter != tupleVals.end(); tupleIter++)
        {
            estimate *= tupleIter->second;
        }
    }
    estimate = estimate / 1000.0; // reverting to old value which is multiplied by 1000
    return estimate;
}

/* computes the estimation process step by step*/
double Statistics::Evaluate(struct OrList *orList, map<string, long> &valuesList)
{
    struct ComparisonOp *comp;
    map<string, double> attSelect;

    while (orList != NULL)
    {
        comp = orList->left;
        string key = string(comp->left->value);
        if (attSelect.find(key) == attSelect.end())
        {
            attSelect[key] = 0.0;
        }
        if (comp->code == 1 || comp->code == 2)
        {
            attSelect[key] = attSelect[key] + 1.0 / 3;
        }
        else
        {
            string lkey = string(comp->left->value);
            long max = valuesList[lkey];
            if (comp->right->code == 4)
            {
                string rkey = string(comp->right->value);
                if (max < valuesList[rkey])
                    max = valuesList[rkey];
            }
            attSelect[key] = attSelect[key] + 1.0 / max;
        }
        orList = orList->rightOr;
    }

    double selectivity = 1.0;
    map<string, double>::iterator iter = attSelect.begin();
    for (; iter != attSelect.end(); iter++)
        selectivity *= (1.0 - iter->second);
    return (1.0 - selectivity);
}

/* Checks whether the arguments passed to evaluate method are valid or not */
bool Statistics::Validate(struct AndList *parseTree, char *relNames[], int numToJoin, map<string, long> &valuesList)
{
    bool result = true;
    while (parseTree != NULL && result)
    {
        struct OrList *head = parseTree->left;
        while (head != NULL && result)
        {
            struct ComparisonOp *comp = head->left;
            if (comp->left->code == 4 && comp->code == 3 && !ContainsAtt(comp->left->value, relNames, numToJoin, valuesList))
            {
                cout << "\n"
                     << comp->left->value << " Does Not Exist";
                result = false;
            }
            if (comp->right->code == 4 && comp->code == 3 && !ContainsAtt(comp->right->value, relNames, numToJoin, valuesList))
                result = false;
            head = head->rightOr;
        }
        parseTree = parseTree->rightAnd;
    }
    if (!result)
        return result;

    map<string, int> tempTable;
    for (int i = 0; i < numToJoin; i++)
    {
        string relName = stats[string(relNames[i])]->GetRelName();
        if (tempTable.find(relName) != tempTable.end())
            tempTable[relName]--;
        else
            tempTable[relName] = stats[string(relNames[i])]->GetRelSize() - 1;
    }

    map<string, int>::iterator tempTableIter = tempTable.begin();
    for (; tempTableIter != tempTable.end(); tempTableIter++)
        if (tempTableIter->second != 0)
        {
            result = false;
            break;
        }
    return result;
}

/* Checks whether attribute is present or not */
bool Statistics::ContainsAtt(char *value, char *relNames[], int numToJoin, map<string, long> &valuesList)
{
    int i = 0;
    while (i < numToJoin)
    {
        map<string, TableInfo *>::iterator iter = stats.find(relNames[i]);
        if (iter != stats.end())
        {
            string key = string(value);
            if (iter->second->GetTableAtts()->find(key) != iter->second->GetTableAtts()->end())
            {
                valuesList[key] = iter->second->GetTableAtts()->find(key)->second;
                return true;
            }
        }
        else
            return false;
        i++;
    }
    return false;
}
