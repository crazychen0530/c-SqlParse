#ifndef SQL_QUERY_PARSER_H
#define SQL_QUERY_PARSER_H

#include "SqlStatement.h"
#include "EngineException.h"

class SqlQueryParser{
    public:
        std::shared_ptr<SqlStatement> parse(const std::string& query);
};

#endif