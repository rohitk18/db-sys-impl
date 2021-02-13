#include "Statistics.h"
#include <gtest/gtest.h>
#include <iostream>
#include <map>

TEST(Statistics, TableInfoCreate) {
    TableInfo ti(25,"nation");


    ASSERT_EQ(ti.GetNumTuples() ,25);
    ASSERT_EQ(ti.GetRelName(), "nation");
    ASSERT_EQ(ti.GetRelSize(), 1);

}

TEST(Statistics, TableInfoCopy)
{
    TableInfo ti(25, "nation");

    TableInfo tnew(ti);

    ASSERT_EQ(tnew.GetNumTuples(), 25);
    ASSERT_EQ(tnew.GetRelName(), "nation");
    ASSERT_EQ(tnew.GetRelSize(), 1);
}

TEST(Statistics, StatisticsCopy)
{
    Statistics s;

    s.AddRel("lineitem", 6001215);
    s.AddAtt("lineitem", "l_returnflag", 3);
    s.AddAtt("lineitem", "l_discount", 11);
    s.AddAtt("lineitem", "l_shipmode", 7);

    Statistics sc(s);
    std::map<string, TableInfo *>::iterator iter = sc.GetStats()->find("lineitem");
    ASSERT_TRUE(iter != sc.GetStats()->end());
    map<string, int>::iterator titer;
    iter = sc.GetStats()->find("lineitem");
    TableInfo *t;
    while(iter!=sc.GetStats()->end()){
        t = iter->second;
        iter++;
    }
    int i = 0;
    titer = t->GetTableAtts()->begin();
    while(titer!=t->GetTableAtts()->end())
        {
            ++i;
            titer++;
        }
        ASSERT_EQ(i, 3);
}

TEST(Statistics, StatisticsAddAttneg1input)
{
    Statistics s;

    s.AddRel("lineitem", 60);
    s.AddAtt("lineitem", "l_returnflag", -1);
    // s.AddAtt("lineitem", "l_discount", 11);
    // s.AddAtt("lineitem", "l_shipmode", 7);

    std::map<string, TableInfo *>::iterator iter = s.GetStats()->find("lineitem");
    TableInfo *t;
    while (iter != s.GetStats()->end())
    {
        t = iter->second;
        iter++;
    }
    map<string, int>::iterator titer =  t->GetTableAtts()->find("l_returnflag");
    int value;
    while (titer != t->GetTableAtts()->end())
    {
        value = titer->second;
        titer++;
    }
    ASSERT_EQ(value, 60);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}