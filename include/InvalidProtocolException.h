#ifndef INVALID_PROTOCOL_EXCEPTION_H
#define INVALID_PROTOCOL_EXCEPTION_H
#include "EngineException.h"

class InvalidProtocolException : public EngineException{
    public:
        InvalidProtocolException() : EngineException(){}
        InvalidProtocolException(const std::string& key) : EngineException(key){}

        InvalidProtocolException(const std::exception& e) : EngineException(e.what()){}
        InvalidProtocolException(const std::string& key,const std::exception& e) : EngineException(key + ": " + e.what()){}
};
#endif