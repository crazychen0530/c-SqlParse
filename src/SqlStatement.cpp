#include "SqlStatement.h"
#include "QueryRelation.h"

SqlStatement::SqlStatement(){
    
}


std::shared_ptr<SqlRelation> SqlStatement::getFrom(){
    return this->from;
}

void SqlStatement::setFrom(std::shared_ptr<SqlRelation> rel){
    this->from = rel;
}

std::shared_ptr<SqlJoinSpec> SqlStatement::getJoin(){
    return this->join;
}

void SqlStatement::setJoin(std::shared_ptr<SqlJoinSpec> spec){
    this->join = spec;
}

std::shared_ptr<SqlRelation> SqlStatement::getInto(){
    return this->into;
}

void SqlStatement::setInto(std::shared_ptr<SqlRelation> into){
    this->into = into;
}

std::string SqlStatement::getSelects(){
    return this->selectAll ? "*" : this->selects->getCacheData()->toString();
}

std::shared_ptr<ExpressionList> SqlStatement::getSelectExpList(){
    return this->selectAll ? nullptr : this->selects->getCacheExpList();
}

std::vector<std::string> SqlStatement::getSelectAliases(){
    return this->selectAll ? std::vector<std::string>() :  this->selects->getCacheExpList()->aliases();
}

void SqlStatement::setSelects(std::string selects){
    if("*" == selects){
        this->selectAll = true;
        this->selects->optional(NullData::INSTANCE);
    }else{
        try{
            this->selects->require(std::make_shared<StringData>(selects),"empty select expression");
        }catch(const EngineException& e){
            throw EngineException(std::string("SQL_SYNTAX_SELECT_") + e.what());
        }
    }
}

std::string SqlStatement::getGroupbys(){
    return this->groupbys->getCacheData()->toString();
}

void SqlStatement::setGroupbys(std::string groupbys){
    try{
        this->groupbys->require(std::make_shared<StringData>(groupbys),"empty group by expression");
    }catch(const EngineException& e){
        throw EngineException(std::string("SQL_SYNTAX_GROUP_BY_") + e.what());
    }
}

std::shared_ptr<SqlWindowSpec> SqlStatement::getWindow(){
    return this->window;
}

void SqlStatement::setWindow(std::shared_ptr<SqlWindowSpec> window){
    this->window = window;
}

std::shared_ptr<IntervalSpec> SqlStatement::getInterval(){
    return this->interval;
}

void SqlStatement::setInterval(std::shared_ptr<IntervalSpec> interval){
    this->interval = interval;
}

std::string SqlStatement::getWhere(){
    return this->where->getCacheData()->toString();
}

void SqlStatement::setWhere(std::string where){
    try{
        this->where->require(std::make_shared<StringData>(where),"empty where expression");
    }catch(const EngineException& e){
        throw EngineException(std::string("SQL_SYNTAX_WHERE_") + e.what());
    }
}

std::string SqlStatement::getHaving(){
    return this->having->getCacheData()->toString();
}

void SqlStatement::setHaving(std::string having){
    try{
        this->having->require(std::make_shared<StringData>(having),"empty having expression");
    }catch(const EngineException& e){
        throw EngineException(std::string("SQL_SYNTAX_HAVING_") + e.what());
    }
}

int SqlStatement::getLimit(){
    return this->limit;
}

void SqlStatement::setLimit(int rows){
    this->limit = rows;
}


std::string SqlStatement::getQuery(){
    return this->query;
}

void SqlStatement::setQuery(const std::string query){
    this->query = query;
}

bool SqlStatement::isSelectAll(){
    if(selectAll){
        return true;
    }
    if(std::dynamic_pointer_cast<QueryRelation>(this->getFrom())){
        auto relation = std::dynamic_pointer_cast<QueryRelation>(this->from);
        auto stmt = relation->getStatement();
        if(!stmt){
            return false;
        }
        if(stmt->isSelectAll()){
            return false;
        }

        auto thisSelectList = this->selects->getCacheExpList();
        auto stmtSelectList = stmt->selects->getCacheExpList();

        if(thisSelectList->size() != stmtSelectList->size()){
            return false;
        }

        std::vector<std::string> thisAliases = thisSelectList->aliases();
        std::vector<std::string> stmtAliases = stmtSelectList->aliases();

        for(int i = 0,s = thisSelectList->size();i < s;++i){
            std::shared_ptr<Expression> exp = thisSelectList->get(i);
            if(!exp->isElement()){
                return false;
            }
            if(!(exp->getElementName() == stmtAliases[i])){
                return false;
            }
            if(!(thisAliases[i] == stmtAliases[i])){
                return false;
            }
        }
        return true;
    }
    return false;
}

bool SqlStatement::isEdgeRunnable(){
    if(!this->groupbys->isEmpty()){
        return false;
    }
    return true;
}
