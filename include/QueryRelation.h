#ifndef QUERY_RELATION_H
#define QUERY_RELATION_H

#include <memory>
#include "XStringUtils.h"
#include "SqlRelation.h"

class SqlStatement;

class QueryRelation : public SqlRelation{
    private: 
        std::shared_ptr<SqlStatement> stmt = nullptr;
        std::string alias = "";
    public:
        std::shared_ptr<SqlStatement> getStatement(){
            return stmt;
        }

        void setStatement(const std::shared_ptr<SqlStatement> stmt){
            this->stmt = stmt;
        }

        std::string getAlias() const override {
            return XStringUtils::isBlank(alias) ? "t" : this->alias;
        }

        void setAlias(const std::string& alias) override {
            this->alias = alias;
        }
};


#endif