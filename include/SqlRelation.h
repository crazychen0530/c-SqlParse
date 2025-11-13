#ifndef SQL_RELATION_H
#define SQL_RELATION_H

#include <string>

class SqlRelation{
    public:
        virtual ~SqlRelation() = default;

        virtual std::string getAlias() const = 0;

        virtual void setAlias(const std::string& alias) = 0;

        virtual std::string toString() const{return "";}
};

#endif