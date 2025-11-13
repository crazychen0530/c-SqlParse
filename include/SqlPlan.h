#ifndef SQL_PLAN_H
#define SQL_PLAN_H

#include <vector>
#include <string>
class SqlPlan{
    private:
        std::vector<std::string> plan;
    public:
        SqlPlan(std::vector<std::string> plan) : plan(plan){}
        const std::vector<std::string>& getPlan() const {
            return plan;
        }
        
        void setPlan(std::vector<std::string> plan){
            this->plan = plan;
        }
        
};

#endif