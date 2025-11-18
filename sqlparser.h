#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <string>
#include <memory>
#include <vector>
#include <nlohmann/json.hpp>

class toolParser{
    public:
        static std::vector<std::string> sql_to_stringplan(const std::string& sql);
        static nlohmann::json sql_to_jsonplan(const std::string& sql);
        static void sql_to_filejson(const std::string& sql);

};



#endif