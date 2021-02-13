#ifndef QUERY_PLANNER_H_
#define QUERY_PLANNER_H_

#include <iostream>
#include <vector>
#include "Schema.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"

#define MAX_RELS 12
#define MAX_RELNAME 50
#define MAX_ATTS 100

using namespace std;

class QueryNode;
/* Query planner deals with parsing , optimizing and compiling SQL */
class QueryPlanner
{
public:
    QueryPlanner(Statistics *st);
    ~QueryPlanner() {}

    void print(ostream &os = cout) ;

private:
    // making new nodes of each operation
    void createLeafs();
    void createJoins();
    void createSums();
    void createProjects();
    void createDistinct();
    void createWrite();
    // ordering of join
    void orderJoins();
    // evaluating ordering of operands
    int evaluateOrder(vector<QueryNode *> operands, Statistics st, int bestFound);
    
    // starter query node
    QueryNode *root;
    // follower query nodes
    vector<QueryNode *> nodes;

    Statistics *stats;
    AndList *used; // reconstruct AndList so that it can be used next time;
                   // should be assigned to boolean after each round
    void listRecycle(AndList *aList) 
    { 
        concatLists(used, aList); 
    }
    static void concatLists(AndList *&left, AndList *&right);

    QueryPlanner(const QueryPlanner &);
};

// node for query; parent node
class QueryNode
{
    friend class QueryPlanner;
    friend class UnaryNode;
    friend class BinaryNode; // passed as argument to binary node
    friend class ProjectNode;
    friend class RemdupliNode;
    friend class JoinNode;
    friend class SumNode;
    friend class GroupByNode;
    friend class WriteNode;

public:
    virtual ~QueryNode();

protected:
    // constructors
    QueryNode(const string &op, Schema *out, Statistics *st);
    QueryNode(const string &op, Schema *out, char *rName, Statistics *st);
    QueryNode(const string &op, Schema *out, char *rNames[], size_t num, Statistics *st);

    // prints output for various operations and nodes
    virtual void print(ostream &os = cout, size_t level = 0);
    virtual void printOperator(ostream &os = cout, size_t level = 0);
    virtual void printSchema(ostream &os = cout, size_t level = 0);
    virtual void printAnnotation(ostream &os = cout, size_t level = 0) = 0; // operator specific
    virtual void printPipe(ostream &os, size_t level = 0) = 0;
    virtual void printChildren(ostream &os, size_t level = 0) = 0;

    // adds satisfied nodes to the and list
    static AndList *pushSelected(AndList *&alist, Schema *target);
    // satisfies particular target
    static bool contains(OrList *ors, Schema *target);
    static bool contains(ComparisonOp *cmp, Schema *target);

    string opName;
    Schema *outSchema;
    char *relNames[MAX_RELS];
    size_t numRels;
    int estimate, cost; // estimated number of tuples and total cost
    Statistics *stats;
    int pout; // output pipe
    static int pipeId;
};

// individual node for each part of query
class LeafNode : private QueryNode
{ // read from file
    friend class QueryPlanner;
    LeafNode(AndList *&boolean, AndList *&pushed, char *relName, char *alias, Statistics *st);
    void printOperator(ostream &os = cout, size_t level = 0);
    void printAnnotation(ostream &os = cout, size_t level = 0);
    void printPipe(ostream &os, size_t level);
    void printChildren(ostream &os, size_t level) {}
    CNF selOp;
    Record literal;
};

// node for operation having children
class UnaryNode : protected QueryNode
{
    friend class QueryPlanner;

protected:
    UnaryNode(const string &opName, Schema *out, QueryNode *c, Statistics *st);
    virtual ~UnaryNode() { delete child; }
    void printPipe(ostream &os, size_t level);
    void printChildren(ostream &os, size_t level) { child->print(os, level + 1); }
    QueryNode *child;
    int inpipe; // input pipe
};

// node for join type operation
class BinaryNode : protected QueryNode
{ // not including set operations.
    friend class QueryPlanner;

protected:
    BinaryNode(const string &opName, QueryNode *l, QueryNode *r, Statistics *st);
    virtual ~BinaryNode()
    {
        delete left;
        delete right;
    }
    void printPipe(ostream &os, size_t level);
    void printChildren(ostream &os, size_t level) 
    {
        left->print(os, level + 1);
        right->print(os, level + 1);
    }
    QueryNode *left;
    QueryNode *right;
    int linpipe, rinpipe; // input pipes
};

// node for project operation
class ProjectNode : private UnaryNode
{
    friend class QueryPlanner;
    ProjectNode(NameList *atts, QueryNode *c);
    void printAnnotation(ostream &os = cout, size_t level = 0);
    int keepMe[MAX_ATTS];
    int numAttsIn, numAttsOut;
};

// node for remove duplication operation
class RemdupliNode : private UnaryNode
{
    friend class QueryPlanner;
    RemdupliNode(QueryNode *c);
    void print(ostream &os = cout, size_t level = 0);
    void printAnnotation(ostream &os = cout, size_t level = 0);
    OrderMaker remdupliOrder;
};

// node for sum operation
class SumNode : private UnaryNode
{
    friend class QueryPlanner;
    SumNode(FuncOperator *parseTree, QueryNode *c);
    Schema *resultSchema(FuncOperator *parseTree, QueryNode *c);
    void printAnnotation(ostream &os = cout, size_t level = 0) ;
    Function f;
};

// node for group by operation
class GroupByNode : private UnaryNode
{
    friend class QueryPlanner;
    GroupByNode(NameList *gAtts, FuncOperator *parseTree, QueryNode *c);
    Schema *resultSchema(NameList *gAtts, FuncOperator *parseTree, QueryNode *c);
    void printAnnotation(ostream &os = cout, size_t level = 0) ;
    OrderMaker groupOrder;
    Function f;
};

// node for join operation
class JoinNode : private BinaryNode
{
    friend class QueryPlanner;
    JoinNode(AndList *&boolean, AndList *&pushed, QueryNode *l, QueryNode *r, Statistics *st);
    void printAnnotation(ostream &os = cout, size_t level = 0);
    CNF selOp;
    Record literal;
};

// operation for printing the output
class WriteNode : private UnaryNode
{
    friend class QueryPlanner;
    WriteNode(FILE *out, QueryNode *c);
    void printAnnotation(ostream &os = cout, size_t level = 0) ;
    FILE *outFile;
};

#endif