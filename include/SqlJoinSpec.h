#ifndef SQL_JOIN_SPEC_H
#define SQL_JOIN_SPEC_H
#include "SqlRelation.h"
#include "SqlJoinType.h"
#include "ExpressionContainer.h"
#include "Expression.h"
#include "StringData.h"
#include "EngineException.h"
class SqlJoinSpec {
    private:
        std::shared_ptr<SqlRelation> relation = nullptr;
        SqlJoinType type;
        std::shared_ptr<ExpressionContainer> condition = std::make_shared<ExpressionContainer>();
    public:
        std::shared_ptr<SqlRelation> getRelation(){
            return this->relation;
        }

        void setRelation(std::shared_ptr<SqlRelation> rel){
            this->relation = rel;
        }

        SqlJoinType getType(){
            return this->type;
        }

        void setType(const SqlJoinType type){
            this->type = type;
        }

        std::string getCondition(){
            return this->condition->getCacheData()->toString();
        }

        std::shared_ptr<Expression> getConditionExp(){
            return this->condition->getCacheExp();
        }

        void setCondition(const std::string exp){
            try{
                this->condition->require(std::make_shared<StringData>(exp),"empty join on expression");
            }catch(const EngineException& e){
                throw EngineException(std::string("SQL_SYNTAX_JOIN_ON_") + e.what());
            }
        }
};

#endif