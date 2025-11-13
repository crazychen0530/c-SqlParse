cmake_minimum_required(VERSION 3.10)
project(sqlplanner_tests)

find_package(GTest REQUIRED)

add_executable(SqlDistributedPlannerTest
    SqlDistributedPlannerTest.cpp
)

add_executable(SqlQueryParserTest
    SqlQueryParserTest.cpp
)

add_executable(SqlQueryPlannerTest
    SqlQueryPlannerTest.cpp
)

target_link_libraries(SqlDistributedPlannerTest
    PRIVATE
    sqlparser
    GTest::gtest_main
    pthread
)

target_link_libraries(SqlQueryParserTest
    PRIVATE
    sqlparser
    GTest::gtest_main
    pthread
)

target_link_libraries(SqlQueryPlannerTest
    PRIVATE
    sqlparser
    GTest::gtest_main
    pthread
)