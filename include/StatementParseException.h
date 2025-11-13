#ifndef STATEMENT_PARSE_EXCEPTION
#define STATEMENT_PARSE_EXCEPTION
#include "EngineException.h"

class StatementParseException : public EngineException{
    public: 
        StatementParseException() : EngineException(){}
        StatementParseException(const std::string& key) : EngineException(key){}

        StatementParseException(const std::exception& e) : EngineException(e.what()){}
        StatementParseException(const std::string& key,const std::exception& e) : EngineException(key + ": " + e.what()){}
};

#endif