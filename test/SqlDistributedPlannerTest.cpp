#include <gtest/gtest.h>
#include "../include/SqlQueryParser.h"
#include "../include/SqlDistributedPlanner.h"
#include "../include/SqlDistributedPlan.h"
#include "../include/SqlStatement.h"

// ========== EdgePlanBasics ==========
TEST(SqlDistributedPlannerTest, EdgePlanBasics) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();

    auto stmt = parser->parse("SELECT a, b, c from t1");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().size(), 2);
    EXPECT_EQ(plan->getEdgePlan()[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan()[1], "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");

    stmt = parser->parse("SELECT a, b, c from t1 where b = 5");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().size(), 3);
    EXPECT_EQ(plan->getEdgePlan()[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan()[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`b = 5`)");
    EXPECT_EQ(plan->getEdgePlan()[2], "Project?id=d_2,input=s_1,output=s_2(selects=`a, b, c`)");
}

// ========== CloudPlanBasics ==========) {
TEST(SqlDistributedPlannerTest, CloudPlanBasics) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();

    auto stmt = parser->parse("SELECT a, sum(b), max(c) from t1 where a > 5 group by a");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 2);
    EXPECT_EQ(plan->getCloudPlan().size(), 1);
    EXPECT_EQ(plan->getEdgePlan()[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan()[1], "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5`)");
    EXPECT_EQ(plan->getCloudPlan()[0],
              "GroupBy?id=d_2,input=s_1,output=s_2(keys=`a`,selects=`a, sum(b), max(c)`)");

    stmt = parser->parse("SELECT a, sum(b) as b, max(c) from t1 group by a having b > 3");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 1);
    EXPECT_EQ(plan->getCloudPlan().size(), 2);
    EXPECT_EQ(plan->getEdgePlan()[0], "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getCloudPlan()[0],
              "GroupBy?id=d_1,input=s_0,output=s_1(keys=`a`,selects=`a, sum(b) as b, max(c)`)");
    EXPECT_EQ(plan->getCloudPlan()[1],
              "Filter?id=d_2,input=s_1,output=s_2(condition=`b > 3`)");
}


TEST(SqlDistributedPlannerTest, EdgeStreamAggregate) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();

    auto stmt = parser->parse("SELECT a, sum(b), max(c) from t1 where a > 5 interval by st every 30 second");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 3);
    EXPECT_EQ(plan->getEdgePlan().at(0),"Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "StreamAggregate?id=d_2,input=s_1,output=s_2(interval=`30`,selects=`a, sum(b), max(c)`,time=`st`,time_unit=`second`)");

    stmt = parser->parse("SELECT a, sum(b), max(c) as d from t1 where a > 5 interval by st every 30 second having d > 10");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 4);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Filter?id=d_1,input=s_0,output=s_1(condition=`a > 5`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "StreamAggregate?id=d_2,input=s_1,output=s_2(interval=`30`,selects=`a, sum(b), max(c) as d`,time=`st`,time_unit=`second`)");
    EXPECT_EQ(plan->getEdgePlan().at(3), "Filter?id=d_3,input=s_2,output=s_3(condition=`d > 10`)");
}


TEST(SqlDistributedPlannerTest, EdgeSubquery) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();

    auto stmt = parser->parse("select a, b " "from  (select a, b, c from t1) t");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 3);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`a, b`)");

    stmt = parser->parse("select a, b, c from  (select a, b, c from t1) t");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 2);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");

    stmt = parser->parse("select a, b from  (select a, b, c from t1) t limit 1000");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 4);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`a, b`)");
    EXPECT_EQ(plan->getEdgePlan().at(3), "Take?id=d_3,input=s_2,output=s_3(rows=`1000`)");

    stmt = parser->parse(
        "SELECT time, sig FROM (SELECT time, ::lst_sig -> sig as lst_sig, sig FROM t1 WHERE sig != null)"
        " WHERE lst_sig = 0 and sig = 1");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 5);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Filter?id=d_1,input=s_0,output=s_1(condition=`sig != null`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`time, ::lst_sig -> sig as lst_sig, sig`)");
    EXPECT_EQ(plan->getEdgePlan().at(3), "Filter?id=d_3,input=s_2,output=s_3(condition=`lst_sig = 0 and sig = 1`)");
    EXPECT_EQ(plan->getEdgePlan().at(4), "Project?id=d_4,input=s_3,output=s_4(selects=`time, sig`)");

    stmt = parser->parse(
        "INSERT INTO t2 SELECT a, b, c FROM  (select a, b, c, d from t1) t WHERE d == 'k' and c > 3");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 5);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Project?id=d_1,input=s_0,output=s_1(selects=`a, b, c, d`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Filter?id=d_2,input=s_1,output=s_2(condition=`d == 'k' and c > 3`)");
    EXPECT_EQ(plan->getEdgePlan().at(3), "Project?id=d_3,input=s_2,output=s_3(selects=`a, b, c`)");
    EXPECT_EQ(plan->getEdgePlan().at(4), "Output?id=d_4,input=s_3(name=`t2`)");
}

TEST(SqlDistributedPlannerTest, CloudSubquery) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();
    auto stmt = parser->parse(
        "SELECT a, sum(b) as b INTO t2 FROM (select a, b, c from t1 where b > 5) t "
        "WHERE a = 4 GROUP BY a HAVING b > 100");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 4);
    EXPECT_EQ(plan->getCloudPlan().size(), 3);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t1`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "Filter?id=d_1,input=s_0,output=s_1(condition=`b > 5`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`a, b, c`)");
    EXPECT_EQ(plan->getEdgePlan().at(3), "Filter?id=d_3,input=s_2,output=s_3(condition=`a = 4`)");
    EXPECT_EQ(plan->getCloudPlan().at(0), "GroupBy?id=d_4,input=s_3,output=s_4(keys=`a`,selects=`a, sum(b) as b`)");
    EXPECT_EQ(plan->getCloudPlan().at(1), "Filter?id=d_5,input=s_4,output=s_5(condition=`b > 100`)");
    EXPECT_EQ(plan->getCloudPlan().at(2), "Output?id=d_6,input=s_5(name=`t2`)");
}

TEST(SqlDistributedPlannerTest, EdgeWindow) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();

    auto stmt = parser->parse(
        "SELECT time, wsum('b') as b from t WINDOW OVER (SLIDING ON 20 HAVING wsize() == 20 )");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 2);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "SlidingWindow?id=d_1,input=s_0,output=s_1(inclusion=`20`,selects=`time, wsum('b') as b`,validity=`wsize() == 20`)");

    stmt = parser->parse(
        "SELECT time, wsum('b') as b from t WINDOW OVER (TUMBLING ON time - wlead('time') < 10000 HAVING wsize() > 10 )");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 2);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "TumblingWindow?id=d_1,input=s_0,output=s_1(inclusion=`time - wlead('time') < 10000`,selects=`time, wsum('b') as b`,validity=`wsize() > 10`)");

    stmt = parser->parse(
        "SELECT a, wlag('b') as b from t WINDOW OVER (PATTERN ON true UNTIL wsize() == 5 PARTITION BY a ORDER BY b HAVING wsize() == 5)");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 2);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "PatternWindow?id=d_1,input=s_0,output=s_1(enter=`true`,exit=`wsize() == 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`,validity=`wsize() == 5`)");

    stmt = parser->parse(
        "SELECT a, wlag('b') as b from t WINDOW OVER (PATTERN ON true UNTIL wsize() == 5 PARTITION BY a ORDER BY b HAVING wsize() == 5) LIMIT 5");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 3);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "PatternWindow?id=d_1,input=s_0,output=s_1(enter=`true`,exit=`wsize() == 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`,validity=`wsize() == 5`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Take?id=d_2,input=s_1,output=s_2(rows=`5`)");

    stmt = parser->parse(
        "SELECT b, a FROM (SELECT a, wlag('b') as b from t WINDOW OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b )) LIMIT 5");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 4);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "SlidingWindow?id=d_1,input=s_0,output=s_1(inclusion=`wsize() < 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Project?id=d_2,input=s_1,output=s_2(selects=`b, a`)");
    EXPECT_EQ(plan->getEdgePlan().at(3), "Take?id=d_3,input=s_2,output=s_3(rows=`5`)");

    stmt = parser->parse(
        "SELECT a, b FROM (SELECT a, wlag('b') as b from t WINDOW OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b )) LIMIT 5");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 3);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(name=`t`)");
    EXPECT_EQ(plan->getEdgePlan().at(1), "SlidingWindow?id=d_1,input=s_0,output=s_1(inclusion=`wsize() < 5`,keys=`a`,selects=`a, wlag('b') as b`,sorts=`b`)");
    EXPECT_EQ(plan->getEdgePlan().at(2), "Take?id=d_2,input=s_1,output=s_2(rows=`5`)");
}

TEST(SqlDistributedPlannerTest, EdgeLimitOptimization) {
    std::shared_ptr<SqlQueryParser> parser = std::make_shared<SqlQueryParser>();
    std::shared_ptr<SqlDistributedPlanner> planner = std::make_shared<SqlDistributedPlanner>();

    auto stmt = parser->parse("SELECT * FROM t1 LIMIT 200");
    auto plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 1);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(limit_count=`200`,max_num_readers=`1`,name=`t1`)");

    stmt = parser->parse("SELECT * FROM (SELECT * FROM t1) t LIMIT 200");
    plan = planner->plan(stmt);
    EXPECT_EQ(plan->getEdgePlan().size(), 1);
    EXPECT_EQ(plan->getCloudPlan().size(), 0);
    EXPECT_EQ(plan->getEdgePlan().at(0), "Input?id=d_0,output=s_0(limit_count=`200`,max_num_readers=`1`,name=`t1`)");
}
