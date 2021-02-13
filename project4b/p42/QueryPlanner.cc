#include <cstring>
#include <climits>
#include <string>
#include <algorithm>
#include "Defs.h"
#include "QueryPlanner.h"

using namespace std;

extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

// from parser.y
extern FuncOperator *finalFunction;
extern TableList *tables;
extern AndList *boolean;
extern NameList *groupingAtts;
extern NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

// helper functions
// pops last vector and returns node
QueryNode *popVector(vector<QueryNode *> &v)
{
    QueryNode *popNode = v.back();
    v.pop_back();
    return popNode;
}

// creates attributes
void createAtt(Attribute &att, char *name, Type type)
{
    att.name = name;
    att.myType = type;
}

// creates indentation for children
string indent(size_t level)
{
    return (string(3 * (level), ' ') + "-> ");
}

// prints details of each component used in operation
string describ(size_t level){
    return (string(3 * (level + 1), ' ') + "* ");
}

QueryPlanner::QueryPlanner(Statistics *st) : stats(st), used(NULL)
{
    createLeafs(); // these nodes read from file
    createJoins();
    createSums();
    createProjects();
    createDistinct();
    createWrite();

    // clean up
    swap(boolean, used);
    print();
}

void QueryPlanner::print(ostream &os)
{
    root->print(os);
}

// makes new leafs for each part of query
void QueryPlanner::createLeafs()
{
    for (TableList *table = tables; table; table = table->next)
    {
        stats->CopyRel(table->tableName, table->aliasAs);
        AndList *pushedAL;
        LeafNode *leafNode = new LeafNode(boolean, pushedAL, table->tableName, table->aliasAs, stats);
        concatLists(used, pushedAL);
        nodes.push_back(leafNode);
    }
}

// makes join nodes if joins exist
void QueryPlanner::createJoins()
{
    orderJoins();
    while (nodes.size() > 1)
    {
        QueryNode *left = popVector(nodes);
        QueryNode *right = popVector(nodes);
        AndList *pushedAL;
        JoinNode *joinNode = new JoinNode(boolean, pushedAL, left, right, stats);
        concatLists(used, pushedAL);
        nodes.push_back(joinNode);
    }
    root = nodes.front();
}

// makes operation nodes for group by, sum and remove duplicate
void QueryPlanner::createSums()
{
    if (groupingAtts)
    {
        if (distinctFunc)
            root = new RemdupliNode(root);
        root = new GroupByNode(groupingAtts, finalFunction, root);
    }
    else if (finalFunction)
    {
        root = new SumNode(finalFunction, root);
    }
}

// makes project nodes for project operation
void QueryPlanner::createProjects()
{
    if (attsToSelect && !finalFunction && !groupingAtts)
        root = new ProjectNode(attsToSelect, root);
}

// makes nodes for remove duplicate operation
void QueryPlanner::createDistinct()
{
    if (distinctAtts)
        root = new RemdupliNode(root);
}

// makes nodes for write to output
void QueryPlanner::createWrite()
{
    root = new WriteNode(stdout, root);
}

// ordering of join nodes
void QueryPlanner::orderJoins()
{
    if (nodes.size() == 0)
        return;
    vector<QueryNode *> operands(nodes);
    sort(operands.begin(), operands.end());
    int minCost = INT_MAX;
    int cost;
    do
    { // traverse all possible permutations
        if ((cost = evaluateOrder(operands, *stats, minCost)) < minCost && cost > 0)
        {
            minCost = cost;
            nodes = operands;
        }
    } while (next_permutation(operands.begin(), operands.end()));
}

// mix each pipe to out
int QueryPlanner::evaluateOrder(vector<QueryNode *> operands, Statistics st, int bestFound)
{
    vector<JoinNode *> list;  // all new nodes made in this simulation; need to be freed
    AndList *recycler = NULL; // AndList used to recycle
    while (operands.size() > 1)
    {
        QueryNode *left = popVector(operands);
        QueryNode *right = popVector(operands);
        AndList *pushed;
        JoinNode *joinNode = new JoinNode(boolean, pushed, left, right, &st);
        concatLists(recycler, pushed);
        operands.push_back(joinNode);
        list.push_back(joinNode);
        if (joinNode->estimate <= 0 || joinNode->cost > bestFound)
            break;
    }
    int cost = operands.back()->cost;
    for (size_t i = 0; i < list.size(); ++i)
    {
        --list[i]->pipeId;
        free(list[i]);
    }                               // recycles pipeIds but do not free children
    concatLists(boolean, recycler); // put the AndLists back for future use
    return operands.back()->estimate < 0 ? -1 : cost;
}

// concatenates lists
void QueryPlanner::concatLists(AndList *&left, AndList *&right)
{
    if (!left)
    {
        swap(left, right);
        return;
    }
    AndList *previous = left, *current = left->rightAnd;
    for (; current; previous = current, current = current->rightAnd)
        ;
    previous->rightAnd = right;
    right = NULL;
}

int QueryNode::pipeId = 0;

QueryNode::QueryNode(const string &op, Schema *out, Statistics *st)
    : opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stats(st), pout(pipeId++)
{
}

QueryNode::QueryNode(const string &op, Schema *out, char *rName, Statistics *st)
    : opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stats(st), pout(pipeId++)
{
    if (rName)
        relNames[numRels++] = strdup(rName);
}

QueryNode::QueryNode(const string &op, Schema *out, char *rNames[], size_t num, Statistics *st)
    : opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stats(st), pout(pipeId++)
{
    for (; numRels < num; ++numRels)
        relNames[numRels] = strdup(rNames[numRels]);
}

QueryNode::~QueryNode()
{
    delete outSchema;
    for (size_t i = 0; i < numRels; ++i)
        delete relNames[i];
}

AndList *QueryNode::pushSelected(AndList *&aList, Schema *target)
{
    AndList header;
    header.rightAnd = aList; // create a list header to avoid handling special cases deleting the first list element
    AndList *current = aList, *previous = &header, *result = NULL;
    for (; current; current = previous->rightAnd)
    {
        if (contains(current->left, target))
        { // should push
            previous->rightAnd = current->rightAnd;
            current->rightAnd = result; // move the node to the result list
            result = current;           // prepend the new node to result list
        }
        else
            previous = current;
    }
    aList = header.rightAnd; // special case: first element moved
    return result;
}

bool QueryNode::contains(OrList *ors, Schema *target)
{
    for (; ors; ors = ors->rightOr)
        if (!contains(ors->left, target))
            return false;
    return true;
}

bool QueryNode::contains(ComparisonOp *cmp, Schema *target)
{
    Operand *left = cmp->left, *right = cmp->right;
    return (left->code != NAME || target->Find(left->value) != -1) &&
           (right->code != NAME || target->Find(right->value) != -1);
}

LeafNode::LeafNode(AndList *&boolean, AndList *&pushed, char *relName, char *alias, Statistics *st) : QueryNode("Select File", new Schema(catalog_path, relName, alias), relName, st)
{
    pushed = pushSelected(boolean, outSchema);
    estimate = stats->ApplyEstimate(pushed, relNames, numRels);
    selOp.GrowFromParseTree(pushed, outSchema, literal);
}

UnaryNode::UnaryNode(const string &opName, Schema *out, QueryNode *c, Statistics *st)
    : QueryNode(opName, out, c->relNames, c->numRels, st), child(c), inpipe(c->pout)
{
}

BinaryNode::BinaryNode(const string &opName, QueryNode *l, QueryNode *r, Statistics *st)
    : QueryNode(opName, new Schema(*l->outSchema, *r->outSchema), st), left(l), right(r), linpipe(left->pout), rinpipe(right->pout)
{
    for (size_t i = 0; i < l->numRels;)
        relNames[numRels++] = strdup(l->relNames[i++]);
    for (size_t j = 0; j < r->numRels;)
        relNames[numRels++] = strdup(r->relNames[j++]);
}

ProjectNode::ProjectNode(NameList *atts, QueryNode *c)
    : UnaryNode("Project", NULL, c, NULL), numAttsIn(c->outSchema->GetNumAtts()), numAttsOut(0)
{
    Schema *cSchema = c->outSchema;
    Attribute resultAtts[MAX_ATTS];
    for (; atts; atts = atts->next, numAttsOut++)
    {
        createAtt(resultAtts[numAttsOut], atts->name, cSchema->FindType(atts->name));
    }
    outSchema = new Schema("", numAttsOut, resultAtts);
}

RemdupliNode::RemdupliNode(QueryNode *c) : UnaryNode("RemoveDuplication", c->outSchema, c, NULL), remdupliOrder(c->outSchema) {}

JoinNode::JoinNode(AndList *&boolean, AndList *&pushed, QueryNode *l, QueryNode *r, Statistics *st) : BinaryNode("Join", l, r, st)
{
    pushed = pushSelected(boolean, outSchema);
    estimate = stats->ApplyEstimate(pushed, relNames, numRels);
    cost = l->cost + estimate + r->cost;
    selOp.GrowFromParseTree(pushed, outSchema, literal);
}

SumNode::SumNode(FuncOperator *parseTree, QueryNode *c) : UnaryNode("Sum", resultSchema(parseTree, c), c, NULL)
{
    f.GrowFromParseTree(parseTree, *c->outSchema);
}

// returns schema for resulting operation
Schema *SumNode::resultSchema(FuncOperator *parseTree, QueryNode *c)
{
    Function fun;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    fun.GrowFromParseTree(parseTree, *c->outSchema);
    return new Schema("", 1, atts[fun.resultType()]);
}

GroupByNode::GroupByNode(NameList *gAtts, FuncOperator *parseTree, QueryNode *c) : UnaryNode("Group by", resultSchema(gAtts, parseTree, c), c, NULL)
{
    groupOrder.growFromParseTree(gAtts, c->outSchema);
    f.GrowFromParseTree(parseTree, *c->outSchema);
}

// returns schema for resulting groupby operation
Schema *GroupByNode::resultSchema(NameList *gAtts, FuncOperator *parseTree, QueryNode *c)
{
    Function fun;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    Schema *cSchema = c->outSchema;
    fun.GrowFromParseTree(parseTree, *cSchema);
    Attribute resultAtts[MAX_ATTS];
    createAtt(resultAtts[0], "sum", fun.resultType());
    int numAtts = 1;
    for (; gAtts; gAtts = gAtts->next, numAtts++)
    {
        createAtt(resultAtts[numAtts], gAtts->name, cSchema->FindType(gAtts->name));
    }
    return new Schema("", numAtts, resultAtts);
}

WriteNode::WriteNode(FILE *out, QueryNode *c)
    : UnaryNode("WriteOut", c->outSchema, c, NULL), outFile(out)
{}

// all output methods
// prints to output for each query node
void QueryNode::print(ostream &os, size_t level)
{
    printOperator(os, level);
    printAnnotation(os, level);
    printSchema(os, level);
    printPipe(os, level);
    printChildren(os, level);
}

void QueryNode::printOperator(ostream &os, size_t level)
{
    os << indent(level) << opName << ": ";
}

void QueryNode::printSchema(ostream &os, size_t level)
{}

void LeafNode::printPipe(ostream &os, size_t level)
{
    os<< "\n" << describ(level) << "Output pipe: " << pout << endl;
}

void UnaryNode::printPipe(ostream &os, size_t level)
{
    os << describ(level) << "Output pipe: " << pout << endl;
    os << describ(level) << "Input pipe: " << inpipe << endl;
}

void BinaryNode::printPipe(ostream &os, size_t level)
{
    os << describ(level) << "Output pipe: " << pout << endl;
    os << describ(level) << "Input pipe: " << linpipe << ", " << rinpipe << endl;
}

void LeafNode::printOperator(ostream &os, size_t level)
{
    os << indent(level) << "Select from " << relNames[0] << ": ";
}

void LeafNode::printAnnotation(ostream &os, size_t level)
{
    // selOp.Print();
}

void ProjectNode::printAnnotation(ostream &os, size_t level)
{
    os << keepMe[0];
    for (size_t i = 1; i < numAttsOut; ++i)
        os << ',' << keepMe[i];
    os << endl;
}

void JoinNode::printAnnotation(ostream &os, size_t level)
{
    selOp.Print();
    os << describ(level) << "Estimate = " << estimate << ", Cost = " << cost << endl;
}

void SumNode::printAnnotation(ostream &os, size_t level)
{
    os << describ(level) << "Function: ";
    (const_cast<Function *>(&f))->Print();
}

void RemdupliNode::print(ostream &os, size_t level)
{
    printOperator(os, level);
    printAnnotation(os, level);
    printSchema(os, level);
    os << "\n";
    printPipe(os, level);
    printChildren(os, level);
}
void RemdupliNode::printAnnotation(ostream &os, size_t level) {}

void GroupByNode::printAnnotation(ostream &os, size_t level)
{
    os << endl
       << describ(level) << "OrderMaker: ";
    (const_cast<OrderMaker *>(&groupOrder))->Print();
    os << describ(level) << "Function: " << endl;
    vector<string> strs = (const_cast<Function *>(&f))->Print();
    while(strs.size()!=0)
    {
        os << string(3 * (level + 2), ' ') << strs.front() << endl;
        strs.erase(strs.begin());
    }
}

void WriteNode::printAnnotation(ostream &os, size_t level)
{
    os << endl << describ(level) << "Output to " << outFile << endl;
}