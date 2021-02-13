#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 50
#define MAX_ORS 50

#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();


#endif

