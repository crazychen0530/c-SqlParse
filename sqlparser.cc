#include "sqlparser.h"
#include "include/SqlQueryPlanner.h"
#include "include/SqlQueryParser.h"

#include <fstream>
#include <vector>
#include <string>
#include <nlohmann/json.hpp>
std::vector<std::string> toolParser::sql_to_stringplan(const std::string& sql){
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt = parser.parse(sql);

    std::shared_ptr<SqlPlan> plan = planner.plan(stmt);

    std::vector<std::string> steps = plan->getPlan();

    return steps;
}

nlohmann::json toolParser::sql_to_jsonplan(const std::string& sql){
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::shared_ptr<SqlStatement> stmt = parser.parse(sql);

    std::shared_ptr<SqlPlan> plan = planner.plan(stmt);

    std::vector<std::string> steps = plan->getPlan();
    nlohmann::json steps_json;
    steps_json["exceeddata"]["job"] = steps;
    return steps_json;
}

void toolParser::sql_to_filejson(const std::string& sql){
    nlohmann::json steps_json = sql_to_jsonplan(sql);
    std::ofstream file("sql_plan.json");
    if(!file.is_open()){
        std::cerr <<"oepn file failed!"<<std::endl;
        return;
    }
    file << steps_json.dump(4);
    file.close();
    std::cout<<"had writen";
}