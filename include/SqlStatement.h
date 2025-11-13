#ifndef SQL_STATEMENT_H
#define SQL_STATEMENT_H
#include <memory>
#include <string>
#include "SqlJoinSpec.h"
#include "SqlWindowSpec.h"
#include "IntervalSpec.h"
#include "SqlRelation.h"
#include "NullData.h"
#include "QueryRelation.h"
#include "../../exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/expression/ExpressionListContainer.h"
#include "../../exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/expression/ExpressionContainer.h"



class SqlStatement {
private:
    std::shared_ptr<SqlRelation> from = nullptr;
    std::shared_ptr<SqlRelation> into = nullptr;
    std::shared_ptr<SqlJoinSpec> join = nullptr;
    std::shared_ptr<SqlWindowSpec> window = nullptr;
    std::shared_ptr<IntervalSpec> interval = nullptr;

    bool selectAll = false;
    std::shared_ptr<ExpressionListContainer> selects = std::make_shared<ExpressionListContainer>();
    std::shared_ptr<ExpressionListContainer> groupbys = std::make_shared<ExpressionListContainer>();
    std::shared_ptr<ExpressionContainer> where = std::make_shared<ExpressionContainer>();
    std::shared_ptr<ExpressionContainer> having = std::make_shared<ExpressionContainer>();
    int limit = 0;

    std::string query;
public:
    SqlStatement();

    std::shared_ptr<SqlRelation> getFrom();

    void setFrom(std::shared_ptr<SqlRelation> rel);

    std::shared_ptr<SqlJoinSpec> getJoin();

    void setJoin(std::shared_ptr<SqlJoinSpec> spec);

    std::shared_ptr<SqlRelation> getInto();

    void setInto(std::shared_ptr<SqlRelation> into);

    std::string getSelects();

    std::shared_ptr<ExpressionList> getSelectExpList();

    std::vector<std::string> getSelectAliases();

    void setSelects(std::string selects);

    std::string getGroupbys();

    void setGroupbys(std::string groupbys);

    std::shared_ptr<SqlWindowSpec> getWindow();

    void setWindow(std::shared_ptr<SqlWindowSpec> window);

    std::shared_ptr<IntervalSpec> getInterval();

    void setInterval(std::shared_ptr<IntervalSpec> interval);

    std::string getWhere();
    void setWhere(std::string where);

    std::string getHaving();

    void setHaving(std::string having);

    int getLimit();

    void setLimit(int rows);
    

    std::string getQuery();

    void setQuery(const std::string query);

    bool isSelectAll();

    bool isEdgeRunnable();

};

#endif