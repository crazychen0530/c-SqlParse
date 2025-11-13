#ifndef SQL_QUERY_PLANER_H
#define SQL_QUERY_PLANER_H

#include <vector>
#include <string>

#include "ClassDefinition.h"
#include "XStringUtils.h"
#include "MutableInt.h"
#include "SqlPlan.h"
#include "QueryRelation.h"
#include "TableRelation.h"
#include "SqlJoinSpec.h"
#include "SqlStatement.h"
#include "SqlWindowSpec.h"
#include "ExpressionModelUtils.h"
#include "PatternWindowSpec.h"
#include "SlidingWindowSpec.h"
#include "TumblingWindowSpec.h"


class SqlQueryPlanner{
    public:
        std::shared_ptr<SqlPlan> plan(const std::shared_ptr<SqlStatement>& stmt);
    protected:
        void plan(const std::shared_ptr<SqlStatement>& stmt,
                std::vector<std::shared_ptr<ClassDefinition>>& steps,
                std::shared_ptr<MutableInt>& currentId,
                std::shared_ptr<MutableInt>& lastSpool);
    private:
        static bool selectOnly(std::shared_ptr<SqlStatement> stmt);
};
#endif