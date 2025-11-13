#include <gtest/gtest.h>
#include "../include/SqlStatement.h"
#include "../include/SqlQueryParser.h"
#include "../include/SqlQueryPlanner.h"

TEST(SqlQueryPlannerTest, Basics) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse("SELECT a, b, c from   t1  ");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 2);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");

    stmt = parser.parse("SELECT * from   t1  ");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 1);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");

    stmt = parser.parse("SELECT * into t2 from   t1  ");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 2);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Output?id=d_1,input=s_0(name=`t2`)");

    stmt = parser.parse("INSERT INTO t2 SELECT a, b from   t1  ;");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Project?id=d_1,input=s_0,output=s_1(selects=`a, b`)");
    EXPECT_EQ(plan[2], "Output?id=d_2,input=s_1(name=`t2`)");

    stmt = parser.parse("SELECT a, b, c from t1 where a > 5 and b = 3");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5 and b = 3`)");
    EXPECT_EQ(plan[2], "Project?id=d_2,input=s_1,output=s_2(selects=`a, b, c`)");

    stmt = parser.parse("SELECT a, b, ' from ' as c from t1 where a > 5 and a['where'] = 3");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5 and a['where'] = 3`)");
    EXPECT_EQ(plan[2], "Project?id=d_2,input=s_1,output=s_2(selects=`a, b, ' from ' as c`)");
}

TEST(SqlQueryPlannerTest, GroupBy) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse("SELECT a, sum(b), max(c) from t1 where a > 5 group by a");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5`)");
    EXPECT_EQ(plan[2], "GroupBy?id=d_2,input=s_1,output=s_2(keys=`a`,selects=`a, sum(b), max(c)`)");

    stmt = parser.parse("SELECT a, sum(b) as b, max(c) from t1 group by a having b > 3");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "GroupBy?id=d_1,input=s_0,output=s_1(keys=`a`,selects=`a, sum(b) as b, max(c)`)");
    EXPECT_EQ(plan[2], "Filter?id=d_2,input=s_1,output=s_2(condition=`b > 3`)");
}

TEST(SqlQueryPlannerTest, StreamAggregate) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse("SELECT a, sum(b), max(c) from t1 where a > 5 interval by st every 30 second");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5`)");
    EXPECT_EQ(plan[2], "StreamAggregate?id=d_2,input=s_1,output=s_2(interval=`30`,selects=`a, sum(b), max(c)`,time=`st`,time_unit=`second`)");

    stmt = parser.parse("SELECT a, sum(b), max(c) as d from t1 where a > 5 interval by st every 30 second having d > 10");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 4);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5`)");
    EXPECT_EQ(plan[2], "StreamAggregate?id=d_2,input=s_1,output=s_2(interval=`30`,selects=`a, sum(b), max(c) as d`,time=`st`,time_unit=`second`)");
    EXPECT_EQ(plan[3], "Filter?id=d_3,input=s_2,output=s_3(condition=`d > 10`)");
}

TEST(SqlQueryPlannerTest, Subquery) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse("select a, b from (select a, b, c from t1) t ");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");
    EXPECT_EQ(plan[2], "Project?id=d_2,input=s_1,output=s_2(selects=`a, b`)");

    stmt = parser.parse("select a, b, c from (select a, b, c from t1) t ");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 2);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");

    stmt = parser.parse("select a, b from (select a, b, c from t1) t limit 1000");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 4);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan[1], "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");
    EXPECT_EQ(plan[2], "Project?id=d_2,input=s_1,output=s_2(selects=`a, b`)");
    EXPECT_EQ(plan[3], "Take?id=d_3,input=s_2,output=s_3(rows=`1000`)");

    stmt = parser.parse(""
                 "SELECT time, sig\n"
                 "  FROM (\n"
                 "    SELECT time, ::lst_sig -> sig as lst_sig, sig \n"
                 "      FROM t1 \n"
                 "      WHERE sig != null\n"
                 "    )\n"
                 "  WHERE lst_sig = 0 and sig = 1");
    plan = planner.plan(stmt)->getPlan();
    EXPECT_EQ(plan.size(), 5);
    EXPECT_EQ(plan.at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan.at(1), "Filter?id=d_1,input=s_0,output=s_1(condition=`sig != null`)");
    EXPECT_EQ(plan.at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`time, ::lst_sig -> sig as lst_sig, sig`)");
    EXPECT_EQ(plan.at(3), "Filter?id=d_3,input=s_2,output=s_3(condition=`lst_sig = 0 and sig = 1`)");
    EXPECT_EQ(plan.at(4), "Project?id=d_4,input=s_3,output=s_4(selects=`time, sig`)");

    stmt = parser.parse(
                "INSERT "
                 "INTO t2 "
                 "SELECT a, b, c "
                 "  FROM  (select a, b, c, d from t1) t "
                 "  WHERE d == 'k' and c > 3");
    plan = planner.plan(stmt)->getPlan();
    EXPECT_EQ(plan.size(), 5);
    EXPECT_EQ(plan.at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan.at(1), "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c, d`)");
    EXPECT_EQ(plan.at(2), "Filter?id=d_2,input=s_1,output=s_2(condition=`d == 'k' and c > 3`)");
    EXPECT_EQ(plan.at(3), "Project?id=d_3,input=s_2,output=s_3(selects=`a, b, c`)");
    EXPECT_EQ(plan.at(4), "Output?id=d_4,input=s_3(name=`t2`)");
    
    //full subquery test case with into, where, having, group by
    stmt = parser.parse(
            "SELECT a, sum(b) as b "
             "INTO t2 "
             "FROM  ("
             "  select a, b, c from  t1 where b > 5  "
             "  ) t "
             "WHERE a = 4 "
             "GROUP BY a "
             "HAVING b > 100"
            );
    plan = planner.plan(stmt)->getPlan();
    EXPECT_EQ(plan.size(), 7);
    EXPECT_EQ(plan.at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan.at(1), "Filter?id=d_1,input=s_0,output=s_1(condition=`b > 5`)");
    EXPECT_EQ(plan.at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`a, b, c`)");
    EXPECT_EQ(plan.at(3), "Filter?id=d_3,input=s_2,output=s_3(condition=`a = 4`)");
    EXPECT_EQ(plan.at(4), "GroupBy?id=d_4,input=s_3,output=s_4(keys=`a`,selects=`a, sum(b) as b`)");
    EXPECT_EQ(plan.at(5), "Filter?id=d_5,input=s_4,output=s_5(condition=`b > 100`)");
    EXPECT_EQ(plan.at(6), "Output?id=d_6,input=s_5(name=`t2`)");

}


TEST(SqlQueryPlannerTest, Window) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse(
        "SELECT time, wsum('b') as b from t "
        "WINDOW OVER (SLIDING ON 20 HAVING wsize() == 20 )");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 2);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan[1], "SlidingWindow?id=d_1,input=s_0,output=s_1(inclusion=`20`,selects=`time, wsum('b') as b`,validity=`wsize() == 20`)");

    stmt = parser.parse(
        "SELECT time, wsum('b') as b from t "
        "WINDOW OVER (TUMBLING ON time - wlead('time') < 10000 HAVING wsize() > 10 )");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 2);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan[1], "TumblingWindow?id=d_1,input=s_0,output=s_1(inclusion=`time - wlead('time') < 10000`,selects=`time, wsum('b') as b`,validity=`wsize() > 10`)");

    stmt = parser.parse(
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (PATTERN ON true UNTIL wsize() == 5 PARTITION BY a ORDER BY b HAVING wsize() == 5)");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 2);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan[1], "PatternWindow?id=d_1,input=s_0,output=s_1(enter=`true`,exit=`wsize() == 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`,validity=`wsize() == 5`)");

    stmt = parser.parse(
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (PATTERN ON true UNTIL wsize() == 5 PARTITION BY a ORDER BY b HAVING wsize() == 5) LIMIT 5");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan[1], "PatternWindow?id=d_1,input=s_0,output=s_1(enter=`true`,exit=`wsize() == 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`,validity=`wsize() == 5`)");
    EXPECT_EQ(plan[2], "Take?id=d_2,input=s_1,output=s_2(rows=`5`)");

    stmt = parser.parse(
        "SELECT b, a FROM ( "
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b ) "
        ") LIMIT 5");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 4);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan[1], "SlidingWindow?id=d_1,input=s_0,output=s_1(inclusion=`wsize() < 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`)");
    EXPECT_EQ(plan[2], "Project?id=d_2,input=s_1,output=s_2(selects=`b, a`)");
    EXPECT_EQ(plan[3], "Take?id=d_3,input=s_2,output=s_3(rows=`5`)");

    stmt = parser.parse(
        "SELECT a, b FROM ( "
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b ) "
        ") LIMIT 5");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan[1], "SlidingWindow?id=d_1,input=s_0,output=s_1(inclusion=`wsize() < 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`)");
    EXPECT_EQ(plan[2], "Take?id=d_2,input=s_1,output=s_2(rows=`5`)");
}

TEST(SqlQueryPlannerTest, ReduceJoin) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse("SELECT s.a, t.b from s JOIN t ON s.a = t.a");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "ReduceJoin?id=d_2,input=s_0,input2=s_1,output=s_2(join_type=`inner`,left_alias=`s`,lefts=`a`,right_alias=`t`,rights=`a`,selects=`s.a, t.b`)");

    stmt = parser.parse("SELECT s.a, t.b from s LEFT JOIN t ON s.a = t.a and t.b = s.c");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "ReduceJoin?id=d_2,input=s_0,input2=s_1,output=s_2(join_type=`left`,left_alias=`s`,lefts=`a,c`,right_alias=`t`,rights=`a,b`,selects=`s.a, t.b`)");

    stmt = parser.parse("SELECT s.a, t.b from s LEFT JOIN t ON s.a = t.a and t.b = s.c and t.d = s.e");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "ReduceJoin?id=d_2,input=s_0,input2=s_1,output=s_2(join_type=`left`,left_alias=`s`,lefts=`a,c,e`,right_alias=`t`,rights=`a,b,d`,selects=`s.a, t.b`)");
} 

TEST(SqlQueryPlannerTest, NestedJoin) {
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt;
    std::vector<std::string> plan;

    stmt = parser.parse("SELECT s.a, t.b from s JOIN t ON s.a > t.a");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "NestedJoin?id=d_2,input=s_0,input2=s_1,output=s_2(condition=`s.a > t.a`,join_type=`inner`,left_alias=`s`,right_alias=`t`,selects=`s.a, t.b`)");

    stmt = parser.parse("SELECT s.a, t.b from s LEFT JOIN t ON s.a = t.a and t.b != s.c");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "NestedJoin?id=d_2,input=s_0,input2=s_1,output=s_2(condition=`s.a = t.a and t.b != s.c`,join_type=`left`,left_alias=`s`,right_alias=`t`,selects=`s.a, t.b`)");

    stmt = parser.parse("SELECT s.a, t.b from s LEFT JOIN t ON s.a = t.a or t.b = s.c and t.d = s.e");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "NestedJoin?id=d_2,input=s_0,input2=s_1,output=s_2(condition=`s.a = t.a or t.b = s.c and t.d = s.e`,join_type=`left`,left_alias=`s`,right_alias=`t`,selects=`s.a, t.b`)");

    stmt = parser.parse("SELECT s.a, t.b from s LEFT JOIN t ON s.a = 1");
    plan = planner.plan(stmt)->getPlan();
    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0], "Input?id=d_0,output=s_0(name=`s`)");
    EXPECT_EQ(plan[1], "Input?id=d_1,output=s_1(name=`t`)");
    EXPECT_EQ(plan[2], "NestedJoin?id=d_2,input=s_0,input2=s_1,output=s_2(condition=`s.a = 1`,join_type=`left`,left_alias=`s`,right_alias=`t`,selects=`s.a, t.b`)");
}
