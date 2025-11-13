#ifndef END_OF_QUERY_EXCEPTION_H
#define END_OF_QUERY_EXCEPTION_H
#include "EngineException.h"

class EndOfQueryException : public EngineException{
    public:
        EndOfQueryException() : EngineException(){}
        EndOfQueryException(const std::string& key) : EngineException(key){}

        EndOfQueryException(const std::exception& e) : EngineException(e.what()){}
        EndOfQueryException(const std::string& key,const std::exception& e) : EngineException(key + ": " + e.what()){}
};
#endif