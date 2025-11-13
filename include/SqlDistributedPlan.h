#ifndef SQL_DISTRIBUTED_PLAN_H
#define SQL_DISTRIBUTED_PLAN_H

#include <vector>
#include <string>
class SqlDistributedPlan{
    private:
        std::vector<std::string> cloudPlan;
        std::vector<std::string> edgePlan;
    public:
        SqlDistributedPlan(std::vector<std::string> cloudPlan,std::vector<std::string> edgePlan) : cloudPlan(cloudPlan),edgePlan(edgePlan){}

        const std::vector<std::string>& getCloudPlan() const {
            return cloudPlan;
        }

        void setCloudPlan(std::vector<std::string> plan){
            cloudPlan = plan;
        }
        const std::vector<std::string>& getEdgePlan() const {
            return edgePlan;
        }

        void setEdgePlan(std::vector<std::string> plan){
            edgePlan = plan;
        }

};

#endif