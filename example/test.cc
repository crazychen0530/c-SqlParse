#include <iostream>
#include "SqlQueryParser.h"
#include "SqlQueryPlanner.h"

int main(){
    SqlQueryParser parser;
    SqlQueryPlanner planner;
    std::string query ;
    while(true){
        std::cout << "Enter SQL Query: ";
        std::getline(std::cin, query);

        std::shared_ptr<SqlStatement> stmt = parser.parse(query);

        std::shared_ptr<SqlPlan> plan = planner.plan(stmt);

        std::vector<std::string> steps = plan->getPlan();
        std::cout<< "SQL  = "<< query << std::endl;
        std::cout<< "Generated Plan = "<< std::endl;
        for(auto &step : steps){
            std::cout<< step<< std::endl;
        }
    }
    return 0;
}