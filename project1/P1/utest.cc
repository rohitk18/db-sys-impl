#include "DBFile.h"
#include "test.h"
#include <gtest/gtest.h>

TEST(DBFile, Create) {
DBFile d;

ASSERT_EQ(d.Create(NULL, heap, NULL), 0);

d.Create("temp.bin", heap, NULL);
FILE* x = fopen("temp.bin", "r");

ASSERT_TRUE(x != NULL);
}

TEST(DBFile, Open) {
DBFile d;

ASSERT_EQ(d.Open(NULL), 0);
d.Create("temp.bin", heap, NULL);
ASSERT_EQ(d.Open("temp.bin"), 1);
FILE* x = fopen("temp.bin", "r");

ASSERT_TRUE(x != NULL);
}

TEST(DBFile, Close) {
DBFile d;
d.Create("temp.bin", heap, NULL);
d.Open("temp.bin");
ASSERT_EQ(d.Close(), 0);
}

TEST(DBFile, FileCreationEmpty) {
    DBFile d;
    Record rec;
    d.Create("temp.bin", heap, NULL);
    ASSERT_EQ(d.GetNext(rec),0);
    d.Close();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
