#ifndef TABLE_RELATION_H
#define TABLE_RELATION_H

#include "XStringUtils.h"
#include "SqlRelation.h"
#include <string>

class TableRelation : public SqlRelation{
    private:
        std::string name = "";
        std::string alias = "";
        bool systemTable = false;

    public:
        std::string getName(){
            return name;
        }

        void setName(const std::string& name){
            this->name = name;
            if(XStringUtils::toLowerCase(XStringUtils::trim(name)).rfind("system.",0) == 0){
                this->systemTable = true;
            }
        }

        std::string getAlias() const override{
            return XStringUtils::isBlank(alias) ? this->name : this->alias;
        }

        void setAlias(const std::string& alias) override{
            this->alias = alias;
        }

        std::string toString() const {
            return name;
        }
        bool isSystemTable() const {
            return systemTable;
        }
};

#endif