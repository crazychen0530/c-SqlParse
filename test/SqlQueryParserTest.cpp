#include <gtest/gtest.h>
#include "../include/SqlQueryParser.h"
#include "../include/SqlStatement.h"
#include "../include/SqlJoinSpec.h"
#include "../include/SqlRelation.h"
#include "../include/TableRelation.h"
#include "../include/QueryRelation.h"
#include "../include/Protocol.h"
#include "../include/ProtocolRelation.h"
#include "../include/PatternWindowSpec.h"
#include "../include/SlidingWindowSpec.h"
#include "../include/TumblingWindowSpec.h"
#include "../../exd-compute-lib-v2/source/exd/pervasive/iot/common/exception/EngineException.h"
#include "../../exd-compute-lib-v2/source/exd/pervasive/iot/common/util/XStringUtils.h"

// ========== SqlQueryParserTest ==========

TEST(SqlQueryParserTest, Basics) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    stmt = parser.parse("SELECT a, b, c from   t1  ");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");

    stmt = parser.parse("SELECT * from   t1  ");
    EXPECT_EQ(stmt->getSelects(), "*");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");

    stmt = parser.parse("SELECT * into t2 from   t1  ");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getInto()->toString(), "t2");

    stmt = parser.parse("INSERT INTO t2 SELECT a, b from   t1  ;");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getInto()->toString(), "t2");

    stmt = parser.parse("SELECT a, b, c from   t1  ;");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getQuery(), "SELECT a, b, c from   t1");

    stmt = parser.parse("SELECT a, b, c from t1 where a > 5 and b = 3");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getWhere(), "a > 5 and b = 3");

    stmt = parser.parse("SELECT a, b, ' from ' as c from t1 where a > 5 and a['where'] = 3");
    EXPECT_EQ(stmt->getSelects(), "a, b, ' from ' as c");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getWhere(), "a > 5 and a['where'] = 3");
}

TEST(SqlQueryParserTest, GroupBy) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    stmt = parser.parse("SELECT a, sum(b), max(c) from t1 where a > 5 group by a");
    EXPECT_EQ(stmt->getSelects(), "a, sum(b), max(c)");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getWhere(), "a > 5");
    EXPECT_EQ(stmt->getGroupbys(), "a");

    stmt = parser.parse("SELECT a, sum(b), max(c) from t1 group by a");
    EXPECT_EQ(stmt->getSelects(), "a, sum(b), max(c)");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getGroupbys(), "a");

    stmt = parser.parse("SELECT a, sum(b) as b, max(c) from t1 group by a having b > 3");
    EXPECT_EQ(stmt->getSelects(), "a, sum(b) as b, max(c)");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getGroupbys(), "a");
    EXPECT_EQ(stmt->getHaving(), "b > 3");
}

TEST(SqlQueryParserTest, StreamAggregate) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    stmt = parser.parse("SELECT st, sum(b), max(c) from t1 where b > 5 interval by st every 30 second");
    EXPECT_EQ(stmt->getSelects(), "st, sum(b), max(c)");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getWhere(), "b > 5");
    EXPECT_EQ(stmt->getInterval()->getInterval(), "st");
    EXPECT_EQ(stmt->getInterval()->getTimeAmount(), 30);
    EXPECT_EQ(stmt->getInterval()->getTimeUnit(), "second");

    stmt = parser.parse("SELECT st, sum(b), max(c) as d from t1 where b > 5 interval by st every 10 minute having d > 5");
    EXPECT_EQ(stmt->getSelects(), "st, sum(b), max(c) as d");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_EQ(stmt->getWhere(), "b > 5");
    EXPECT_EQ(stmt->getInterval()->getInterval(), "st");
    EXPECT_EQ(stmt->getInterval()->getTimeAmount(), 10);
    EXPECT_EQ(stmt->getInterval()->getTimeUnit(), "minute");
    EXPECT_EQ(stmt->getHaving(), "d > 5");
}

TEST(SqlQueryParserTest, TableAlias) {
    SqlQueryParser parser;
    auto stmt = parser.parse("SELECT a, b, c from t1 t");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t");
}

TEST(SqlQueryParserTest, AliasNamingSyntax) {
    SqlQueryParser parser;
    auto stmt = parser.parse("SELECT a, b, c from t1 t_1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t_1");

    stmt = parser.parse("SELECT a, b, c from t1 t.1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t.1");

    stmt = parser.parse("SELECT a, b, c from t1 t.0x45.1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t.0x45.1");

    stmt = parser.parse("SELECT a, b, c from t1 t-1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t-1");
}

TEST(SqlQueryParserTest, Subquery) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;
    std::shared_ptr<SqlStatement> substmt;
    std::shared_ptr<SqlStatement> substmt2;

    stmt = parser.parse("SELECT a, b, c from  (select a, b, c from t1) t ");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement()->getQuery(), "select a, b, c from t1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t");

    stmt = parser.parse("SELECT a, b, c from  (select a, b, c from  t1  ) t ");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement()->getQuery(), "select a, b, c from  t1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t");

    // two-level subquery without alias in inside subquery
    stmt = parser.parse("SELECT a, b, c from  (select a, b, c from (select a, b, c from t1)) t ");
    substmt = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement();
    substmt2 = std::dynamic_pointer_cast<QueryRelation>(substmt->getFrom())->getStatement();
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(substmt2->getQuery(), "select a, b, c from t1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t");

    // two-level subquery with alias in inside subquery
    stmt = parser.parse("SELECT a, b, c from  (select a, b, c from (select a, b, c from t1) s) t ");
    substmt = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement();
    substmt2 = std::dynamic_pointer_cast<QueryRelation>(substmt->getFrom())->getStatement();
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(substmt2->getQuery(), "select a, b, c from t1");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t");
    EXPECT_EQ(substmt->getFrom()->getAlias(), "s");

    // one-level group-by inside subquery
    stmt = parser.parse("SELECT a, b, c from  (select a, max(b) as b, sum(c) as c from t1 group by a) t ");
    substmt = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement();
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(substmt->getQuery(), "select a, max(b) as b, sum(c) as c from t1 group by a");
    EXPECT_EQ(substmt->getSelects(), "a, max(b) as b, sum(c) as c");
    EXPECT_EQ(substmt->getGroupbys(), "a");
    EXPECT_EQ(stmt->getFrom()->getAlias(), "t");

    // one-level join inside subquery
    stmt = parser.parse("SELECT a, b, c from  (select s.a, t.b, t.c from t1 s join t2 t on s.a = t.a) t ");
    substmt = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement();
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(substmt->getQuery(), "select s.a, t.b, t.c from t1 s join t2 t on s.a = t.a");
    EXPECT_EQ(substmt->getFrom()->toString(), "t1");
    EXPECT_EQ(substmt->getFrom()->getAlias(), "s");
    EXPECT_EQ(substmt->getSelects(), "s.a, t.b, t.c");
    std::vector<std::string> expectedAliases = {"a", "b", "c"};
    EXPECT_EQ(substmt->getSelectAliases(), expectedAliases);
    EXPECT_EQ(substmt->getJoin()->getRelation()->toString(), "t2");
    EXPECT_EQ(substmt->getJoin()->getRelation()->getAlias(), "t");

    // window in subquery
    stmt = parser.parse(
        "SELECT a,b FROM ( "
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b ) "
        ") LIMIT 5");
    substmt = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement();
    EXPECT_EQ(substmt->getSelects(), "a, wlag('b') as b");
    EXPECT_EQ(substmt->getWindow()->getKeys(), "a");
    EXPECT_EQ(substmt->getWindow()->getSorts(), "b");
    EXPECT_EQ(std::dynamic_pointer_cast<SlidingWindowSpec>(substmt->getWindow())->getInclusion(), "wsize() < 5");
    EXPECT_EQ(stmt->getLimit(), 5);
}

TEST(SqlQueryParserTest, Join) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;
    std::shared_ptr<SqlJoinSpec> join;

    // inner join (default)
    stmt = parser.parse("SELECT s.a, s.b, t.c from s join t on s.b = t.b");
    join = stmt->getJoin();
    EXPECT_EQ(stmt->getSelects(), "s.a, s.b, t.c");
    EXPECT_EQ(XStringUtils::toLowerCase(toString(join->getType())), "inner");
    EXPECT_EQ(join->getCondition(), "s.b = t.b");
    EXPECT_EQ(std::dynamic_pointer_cast<TableRelation>(stmt->getFrom())->getAlias(), "s");
    EXPECT_EQ(std::dynamic_pointer_cast<TableRelation>(join->getRelation())->getAlias(), "t");

    // explicit inner join
    stmt = parser.parse("SELECT s.a, s.b, t.c from s inner join t on s.b = t.b");
    join = stmt->getJoin();
    EXPECT_EQ(XStringUtils::toLowerCase(toString(join->getType())), "inner");
    EXPECT_EQ(join->getCondition(), "s.b = t.b");

    // left join
    stmt = parser.parse("SELECT s.a, s.b, t.c from s left join t on s.b > t.b and t.c = null");
    join = stmt->getJoin();
    EXPECT_EQ(XStringUtils::toLowerCase(toString(join->getType())), "left");
    EXPECT_EQ(join->getCondition(), "s.b > t.b and t.c = null");

    // right outer join
    stmt = parser.parse("SELECT s.a, s.b, t.c from s right outer join t on s.b = t.c");
    join = stmt->getJoin();
    EXPECT_EQ(XStringUtils::toLowerCase(toString(join->getType())), "right");
    EXPECT_EQ(join->getCondition(), "s.b = t.c");

    // full outer join with subqueries
    stmt = parser.parse("SELECT s.a, s.b, t.c from (select * from t1) s full outer join (select * from t2) t on s.b = t.c");
    join = stmt->getJoin();
    EXPECT_EQ(XStringUtils::toLowerCase(toString(join->getType())), "full");
    EXPECT_EQ(std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement()->getQuery(), "select * from t1");
    EXPECT_EQ(std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getAlias(), "s");
    EXPECT_EQ(std::dynamic_pointer_cast<QueryRelation>(join->getRelation())->getStatement()->getQuery(), "select * from t2");
    EXPECT_EQ(join->getCondition(), "s.b = t.c");
}

TEST(SqlQueryParserTest, Window) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    // Sliding window test 1
    stmt = parser.parse(
        "SELECT time, wsum('b') as b from t "
        "WINDOW OVER (SLIDING ON 20 HAVING wsize() == 20 )"
    );
    EXPECT_EQ(stmt->getSelects(), "time, wsum('b') as b");
    EXPECT_EQ(std::dynamic_pointer_cast<SlidingWindowSpec>(stmt->getWindow())->getInclusion(), "20");
    EXPECT_EQ(std::dynamic_pointer_cast<SlidingWindowSpec>(stmt->getWindow())->getHaving(), "wsize() == 20");

    // Tumbling window test
    stmt = parser.parse(
        "SELECT time, wsum('b') as b from t "
        "WINDOW OVER (TUMBLING ON time - wlead('time') < 10000 HAVING wsize() > 10 )"
    );
    EXPECT_EQ(stmt->getSelects(), "time, wsum('b') as b");
    EXPECT_EQ(std::dynamic_pointer_cast<TumblingWindowSpec>(stmt->getWindow())->getInclusion(),
              "time - wlead('time') < 10000");

    // Sliding window with partition/order
    stmt = parser.parse(
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b )"
    );
    EXPECT_EQ(stmt->getSelects(), "a, wlag('b') as b");
    EXPECT_EQ(stmt->getWindow()->getKeys(), "a");
    EXPECT_EQ(stmt->getWindow()->getSorts(), "b");
    EXPECT_EQ(std::dynamic_pointer_cast<SlidingWindowSpec>(stmt->getWindow())->getInclusion(), "wsize() < 5");

    // Pattern window with having
    stmt = parser.parse(
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (PATTERN ON true UNTIL wsize() == 5 PARTITION BY a ORDER BY b HAVING wsize() == 5)"
    );
    EXPECT_EQ(stmt->getSelects(), "a, wlag('b') as b");
    EXPECT_EQ(stmt->getWindow()->getKeys(), "a");
    EXPECT_EQ(stmt->getWindow()->getSorts(), "b");
    EXPECT_EQ(stmt->getWindow()->getHaving(), "wsize() == 5");
    EXPECT_EQ(std::dynamic_pointer_cast<PatternWindowSpec>(stmt->getWindow())->getEnter(), "true");
    EXPECT_EQ(std::dynamic_pointer_cast<PatternWindowSpec>(stmt->getWindow())->getExit(), "wsize() == 5");

    // Pattern window without partition/order
    stmt = parser.parse(
        "SELECT a, wlag('b') as b from t "
        "WINDOW OVER (PATTERN ON true UNTIL wsize() == 5 HAVING wsize() == 5)"
    );
    EXPECT_EQ(stmt->getSelects(), "a, wlag('b') as b");
    EXPECT_EQ(stmt->getWindow()->getHaving(), "wsize() == 5");
    EXPECT_EQ(std::dynamic_pointer_cast<PatternWindowSpec>(stmt->getWindow())->getEnter(), "true");
    EXPECT_EQ(std::dynamic_pointer_cast<PatternWindowSpec>(stmt->getWindow())->getExit(), "wsize() == 5");

    // Session + Sliding + limit
    stmt = parser.parse(
        "SELECT a, wlag('b') as b from t "
        "SESSION OVER (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b ) LIMIT 5"
    );
    EXPECT_EQ(stmt->getSelects(), "a, wlag('b') as b");
    EXPECT_EQ(stmt->getWindow()->getKind(), "SESSION");
    EXPECT_EQ(stmt->getWindow()->getKeys(), "a");
    EXPECT_EQ(stmt->getWindow()->getSorts(), "b");
    EXPECT_EQ(std::dynamic_pointer_cast<SlidingWindowSpec>(stmt->getWindow())->getInclusion(), "wsize() < 5");
    EXPECT_EQ(stmt->getLimit(), 5);
}

TEST(SqlQueryParserTest, Limit) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    // Test simple subquery with limit 100
    stmt = parser.parse("SELECT a, b, c from (select a, b, c from t1) t limit 100");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(stmt->getLimit(), 100);

    // Test subquery with group by and having, limit 200
    stmt = parser.parse("SELECT a, b, c from (select a, b, c from t1) t group by a, b, c having b > 100 limit 200");
    EXPECT_EQ(stmt->getSelects(), "a, b, c");
    EXPECT_EQ(stmt->getLimit(), 200);
}

TEST(SqlQueryParserTest, SpecialExpressions) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    stmt = parser.parse("SELECT a, e'b' as b, n'c' as c, d['e'] as e, e[5] as e5 from t1");
    EXPECT_EQ(stmt->getSelects(), "a, e'b' as b, n'c' as c, d['e'] as e, e[5] as e5");
}

TEST(SqlQueryParserTest, CommentRemoval) {
    SqlQueryParser parser;
    std::shared_ptr<SqlStatement> stmt;

    stmt = parser.parse(
        "/* Capture the change for sig from 0 to 1 */\n"
        "SELECT time, sig\n"
        "  FROM (\n"
        "    SELECT time, ::lst_sig -> sig as lst_sig, sig \n"
        "      FROM t1 /* record the last sig value */\n"
        "      WHERE sig != null\n"
        "    )\n"
        "  WHERE lst_sig = 0 and sig = 1"
    );

    EXPECT_EQ(stmt->getSelects(), "time, sig");
    EXPECT_EQ(stmt->getWhere(), "lst_sig = 0 and sig = 1");

    auto from = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom());
    ASSERT_NE(from, nullptr);
    EXPECT_EQ(from->getStatement()->getSelects(), "time, ::lst_sig -> sig as lst_sig, sig");
}


TEST(SqlQueryParserTest, Protocol) {
    SqlQueryParser parser;
    auto stmt = parser.parse("SELECT * into protocol.vsw from   t1  ");
    EXPECT_EQ(stmt->getFrom()->toString(), "t1");
    EXPECT_TRUE(std::dynamic_pointer_cast<ProtocolRelation>(stmt->getInto()) != nullptr);
    EXPECT_EQ(std::dynamic_pointer_cast<ProtocolRelation>(stmt->getInto())->getProtocol(), Protocol::VSW);
}


TEST(SqlQueryParserTest, SelectError) {
    SqlQueryParser parser;

    try {
        parser.parse("foo ");
        FAIL() << "missing SELECT";
    } catch (const EngineException& e) {
        EXPECT_STREQ("SQL_SYNTAX_INVALID_SQL_SYNTAX: foo", e.what());
    }

    try {
        parser.parse("SELECT ");
        FAIL() << "missing from";
    } catch (const EngineException& e) {
        EXPECT_STREQ("SQL_SYNTAX_MISSING_EXPRESSIONS_AFTER_SELECT", e.what());
    }

    try {
        parser.parse("SELECT a");
        FAIL() << "missing from";
    } catch (const EngineException& e) {
        EXPECT_STREQ("SQL_SYNTAX_UNEXPECTED_END_WITHOUT_FROM_OR_INTO", e.what());
    }

    try {
        parser.parse("SELECT a, b, c");
        FAIL() << "missing from";
    } catch (const EngineException& e) {
        EXPECT_STREQ("SQL_SYNTAX_UNEXPECTED_END_WITHOUT_FROM_OR_INTO", e.what());
    }

    try {
        parser.parse("SELECT a, b, c fro");
        FAIL() << "missing from";
    } catch (const EngineException& e) {
        EXPECT_STREQ("SQL_SYNTAX_UNEXPECTED_END_WITHOUT_FROM_OR_INTO", e.what());
    }
}


TEST(SqlQueryParserTest, FromError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b, c from");
        FAIL() << "Expected EngineException (missing relation after from)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_MISSING_RELATION_AFTER_FROM");
    }

    try {
        parser.parse("SELECT a, b, c from  ");
        FAIL() << "Expected EngineException (missing relation after from)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_MISSING_RELATION_AFTER_FROM");
    }
}


TEST(SqlQueryParserTest, WhereError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b, c from exd where");
        FAIL() << "Expected EngineException (missing condition after where)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_MISSING_EXPRESSIONS_AFTER_WHERE");
    }

    try {
        parser.parse("SELECT a, b, c from exd where a = b, c = d");
        FAIL() << "Expected EngineException (unexpected comma in where)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_WHERE_EXPRESSION_COMMA_EXTRA");
    }
}

TEST(SqlQueryParserTest, KeywordPlacementError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, sum(b), sum(c) where exd from c");
        FAIL() << "Expected EngineException (invalid group before where)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: WHERE");
    }

    try {
        parser.parse("SELECT a, sum(b), sum(c) from exd group by a where a > 5");
        FAIL() << "Expected EngineException (invalid group before where)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: WHERE");
    }

    try {
        parser.parse("SELECT a, sum(b), sum(c) from exd having a > 3 where a > 5");
        FAIL() << "Expected EngineException (invalid having without group by)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: HAVING");
    }

    try {
        parser.parse("SELECT a, sum(b), sum(c) from exd having a > 3 where a > 5");
        FAIL() << "Expected EngineException (invalid having without group by)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: HAVING");
    }
}


TEST(SqlQueryParserTest, KeywordAndCommaError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, from(b) from exd");
        FAIL() << "Expected EngineException (invalid from keyword as function name)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_TOO_MANY_COMMA_AFTER_SELECT");
    }

    try {
        parser.parse("SELECT a, having(b) from exd");
        FAIL() << "Expected EngineException (invalid having keyword as function name)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: HAVING");
    }

    try {
        parser.parse("SELECT a, sum(b) from exd group by a,");
        FAIL() << "Expected EngineException (too many commas after group by)";
    } catch (const EngineException& e) {
        EXPECT_STREQ(e.what(), "SQL_SYNTAX_TOO_MANY_COMMA_AFTER_GROUP_BY");
    }
}

TEST(SqlQueryParserTest, ParentheseError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b) from exd");
        FAIL() << "Expected EngineException: invalid )";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string("SQL_SYNTAX_UNEXPECTED_PARENTHESE_CLOSE"), e.what());
    }

    try {
        parser.parse("SELECT a, b from exd)");
        FAIL() << "Expected EngineException: invalid )";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string("SQL_SYNTAX_UNEXPECTED_PARENTHESE_CLOSE"), e.what());
    }

    try {
        parser.parse("SELECT a, b from (select a from exd)) t");
        FAIL() << "Expected EngineException: invalid )";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string("SQL_SYNTAX_TOO_MANY_SUBQUERY_PARENTHESE_CLOSE"), e.what());
    }

    try {
        parser.parse("SELECT a, b from (select a from exd) t)");
        FAIL() << "Expected EngineException: invalid )";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string("SQL_SYNTAX_INVALID_FROM_SYNTAX: )"), e.what());
    }

    try {
        parser.parse("SELECT a, b from (select a from exd) t/");
        FAIL() << "Expected EngineException: invalid )";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string("SQL_SYNTAX_INVALID_FROM_SYNTAX: /"), e.what());
    }
}

TEST(SqlQueryParserTest, SemicolonError) {
    SqlQueryParser parser;
    try {
        parser.parse("SELECT a, b; from exd");
        FAIL() << "Expected EngineException: SQL_SYNTAX_UNEXPECTED_END_WITHOUT_INTO_OR_FROM";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_UNEXPECTED_END_WITHOUT_FROM_OR_INTO");
    }

    try {
        parser.parse("SELECT a, b from exd;where");
        FAIL() << "Expected EngineException: SQL_SYNTAX_UNEXPECTED_CONTENT_AFTER_END";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_UNEXPECTED_CONTENT_AFTER_END");
    }
}

TEST(SqlQueryParserTest, StarError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b from *");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_STAR_WITH_FROM";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_STAR_WITH_FROM");
    }

    try {
        parser.parse("SELECT a, b from exd where *");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_STAR_WITH_WHERE";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_STAR_WITH_WHERE");
    }
}

TEST(SqlQueryParserTest, IntoError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b from (select a, b into t2 from t1)");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INTO_NOT_ALLOWED_IN_SUBQUERY";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INTO_NOT_ALLOWED_IN_SUBQUERY");
    }

    try {
        parser.parse("INSERT INTO t2 SELECT a, b INTO t2 from t1");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INTO_ALREADY_DEFINED";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INTO_ALREADY_DEFINED");
    }

    try {
        parser.parse("SELECT * into protocol.bad from   t1  ");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_PROTOCOL: protocol.bad";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_PROTOCOL: protocol.bad");
    }

    try {
        parser.parse("SELECT * into system.tables from   t1  ");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INSERT_NOT_ALLOWED_WITH_SYSTEM_TABLES";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INSERT_NOT_ALLOWED_WITH_SYSTEM_TABLES");
    }
}

TEST(SqlQueryParserTest, LimitError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b from (select a, b from t1 limit 100)");
        FAIL() << "Expected EngineException: SQL_SYNTAX_LIMIT_NOT_ALLOWED_IN_SUBQUERY";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_LIMIT_NOT_ALLOWED_IN_SUBQUERY");
    }

    try {
        parser.parse("INSERT INTO t2 SELECT a, b from t1 limit -100");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_LIMIT_NUMBER: -100";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_LIMIT_NUMBER: -100");
    }

    try {
        parser.parse("INSERT INTO t2 SELECT a, b from t1 limit 100 200");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_CONTENT_AFTER_LIMIT";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_CONTENT_AFTER_LIMIT");
    }
}

TEST(SqlQueryParserTest, WindowError) {
    SqlQueryParser parser;

    try {
        parser.parse(
            "SELECT a, wlag('b') as b from t "
            "WINDOW (SLIDING ON wsize() < 5 PARTITION BY a ORDER BY b )");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_WINDOW_OVER_SYNTAX: (";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_WINDOW_OVER_SYNTAX: (");
    }

    try {
        parser.parse(
            "SELECT a, wlag('b') as b from t "
            "WINDOW OVER(FOO ON wsize() < 5 PARTITION BY a ORDER BY b )");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_WINDOW_OVER_SYNTAX: FOO";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_WINDOW_OVER_SYNTAX: FOO");
    }

    try {
        parser.parse(
            "SELECT a, wlag('b') as b from t "
            "WINDOW OVER(SLIDING ON wsize() < 5 PARTITION BY a HAVING a > 5 )");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: HAVING";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: HAVING");
    }
}

TEST(SqlQueryParserTest, SubqueryError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b, c from (select a, b, c from (select a, b, c from t1) t foo) t ");
        FAIL() << "Expected EngineException: SQL_SYNTAX_SUBQUERY_MISSING_PARENTHESE_AFTER: t";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_SUBQUERY_MISSING_PARENTHESE_AFTER: t");
    }

    try {
        parser.parse("SELECT a, b, c from (select a, b, c from (select a, b, c from t1) limit) t ");
        FAIL() << "Expected EngineException: SQL_SYNTAX_SUBQUERY_ALIAS_USING_RESERVED_WORD: limit";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_SUBQUERY_ALIAS_USING_RESERVED_WORD: limit");
    }
}

TEST(SqlQueryParserTest, ValidateError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT . from exd ");
        FAIL() << "Expected EngineException: SQL_SYNTAX_SELECT_EXPRESSION_CHAR_UNRECOGNIZED: '.' near { . }";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_SELECT_EXPRESSION_CHAR_UNRECOGNIZED: '.' near { . }");
    }

    try {
        parser.parse("SELECT a, b, foo(c) from exd ");
        FAIL() << "Expected EngineException: SQL_SYNTAX_SELECT_EXPRESSION_FUNCTION_NOT_FOUND: 'foo' near { foo(c) }";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_SELECT_EXPRESSION_FUNCTION_NOT_FOUND: 'foo' near { foo(c) }");
    }
}


TEST(SqlQueryParserTest, AliasNamingError) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT a, b, c from t1 _1");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: _1";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: _1");
    }

    try {
        parser.parse("SELECT a, b, c from t1 _.a");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: _.a";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: _.a");
    }

    try {
        parser.parse("SELECT a, b, c from t1 _.a.a");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: _.a.a";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: _.a.a");
    }

    try {
        parser.parse("SELECT a, b, c from t1 .");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: .";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: .");
    }

    try {
        parser.parse("SELECT a, b, c from t1 .aa");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: .aa";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: .aa");
    }

    try {
        parser.parse("SELECT a, b, c from t1 aa.");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: aa.";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: aa.");
    }

    try {
        parser.parse("SELECT a, b, c from t1 1");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: 1";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: 1");
    }

    try {
        parser.parse("SELECT a, b, c from t1 1234");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: 1234";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: 1234");
    }

    try {
        parser.parse("SELECT a, b, c from t1 a+b");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: a+b";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: a+b");
    }

    try {
        parser.parse("SELECT a, b, c from t1 -5");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: -5";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: -5");
    }

    try {
        parser.parse("SELECT a, b, c from t1 a-");
        FAIL() << "Expected EngineException: SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: a-";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: a-");
    }
}

TEST(SqlQueryParserTest, TestJoinUnsupportedKeywords) {
    SqlQueryParser parser;

    try {
        parser.parse("SELECT s.a, s.b, t.c from s left join t on s.b = t.b where s.b = 3");
        FAIL() << "Expected EngineException: SQL_SYNTAX_WORDS_AFTER_JOIN_NOT_YET_SUPPORTED: where";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_WORDS_AFTER_JOIN_NOT_YET_SUPPORTED: where");
    }

    try {
        parser.parse("select * from (SELECT s.a, s.b, t.c from s left join t on s.b = t.b group by s.a) t");
        FAIL() << "Expected EngineException: SQL_SYNTAX_WORDS_AFTER_JOIN_NOT_YET_SUPPORTED: group";
    } catch (const EngineException& e) {
        EXPECT_EQ(std::string(e.what()), "SQL_SYNTAX_WORDS_AFTER_JOIN_NOT_YET_SUPPORTED: group");
    }
}
