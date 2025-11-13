#ifndef SQL_DISTRIBUTED_PLANER_H
#define SQL_DISTRIBUTED_PLANER_H

#include <vector>
#include <string>

#include "ClassDefinition.h"
#include "XStringUtils.h"
#include "MutableInt.h"
#include "SqlDistributedPlan.h"
#include "QueryRelation.h"
#include "TableRelation.h"
#include "SqlJoinSpec.h"
#include "SqlStatement.h"
#include "SqlWindowSpec.h"
#include "ExpressionModelUtils.h"
#include "PatternWindowSpec.h"
#include "SlidingWindowSpec.h"
#include "TumblingWindowSpec.h"

class SqlDistributedPlanner{
    public:
        std::shared_ptr<SqlDistributedPlan> plan(const std::shared_ptr<SqlStatement>& stmt);

    protected:
        void plan(const std::shared_ptr<SqlStatement>& stmt,
                std::vector<std::shared_ptr<ClassDefinition>>& cloudSteps,
                std::vector<std::shared_ptr<ClassDefinition>>& edgeSteps,
                std::shared_ptr<MutableInt> edgeRunnable,
                std::shared_ptr<MutableInt> currentId,
                std::shared_ptr<MutableInt> lastSpool);

    private:
        void addStep(const std::shared_ptr<SqlStatement>& stmt,
                    const std::shared_ptr<ClassDefinition>& step,
                    std::vector<std::shared_ptr<ClassDefinition>>& cloudSteps,
                    std::vector<std::shared_ptr<ClassDefinition>>& edgeSteps,
                    std::shared_ptr<MutableInt> edgeRunnable,
                    bool switchOverPushdown);

    static bool selectOnly(const std::shared_ptr<SqlStatement>& stmt);  
};


#endif