#ifndef SQL_SYNTAXUTILS_H
#define SQL_SYNTAXUTILS_H

#include "SqlFragment.h"
#include "SqlJoinType.h"
#include "SqlWindowType.h"
#include <string>

class SqlSyntaxUtils final {
    private:
        SqlSyntaxUtils();

    public:
        static bool isWhiteSpace(char c);
    
        static bool isReservedKeyword(const std::string& word);

        static bool canExitOnEnd(SqlFragment fragment);

        static bool canBeExpressions(SqlFragment fragment);

        static bool isValidNameOrAlias(const std::string& word);

        static SqlJoinType getJoinType(const std::string& word);

        static SqlWindowType getWindowType(const std::string& word);
};

#endif