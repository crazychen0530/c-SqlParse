#include "../include/SqlSyntaxUtils.h"

#include "XStringUtils.h"
SqlSyntaxUtils::SqlSyntaxUtils(){}

bool SqlSyntaxUtils::isWhiteSpace(char c){
    return c == ' ' || c == '\t' || c== '\n' || c == '\r';
}

bool SqlSyntaxUtils::isReservedKeyword(const std::string& word){
    std::string myword = XStringUtils::toLowerCase(word);
    if( myword == "by" ||
        myword == "every" ||
        myword == "from" ||
        myword == "full" ||
        myword == "group" ||
        myword == "having" ||
        myword == "inner" ||
        myword == "interval" ||
        myword == "into" ||
        myword == "join" ||
        myword == "left" ||
        myword == "limit" ||
        myword == "on" ||
        myword == "order" ||
        myword == "outer" ||
        myword == "over" ||
        myword == "partition" ||
        myword == "right" ||
        myword == "select" ||
        myword == "session" ||
        myword == "until" ||
        myword == "where" ||
        myword == "window"){
            return true;
        }else{
            return false;
        }
}


bool SqlSyntaxUtils::canExitOnEnd(SqlFragment fragment){
    return fragment != SqlFragment::SELECT;
}


bool SqlSyntaxUtils::canBeExpressions(SqlFragment fragment){
    return fragment == SqlFragment::SELECT
            || fragment == SqlFragment::GROUP_BY
            || fragment == SqlFragment::PARTITION_BY
            || fragment == SqlFragment::ORDER_BY;
}

bool SqlSyntaxUtils::isValidNameOrAlias(const std::string& word){
    const int len = word.size();
    if(len == 0){
        return false;
    } else if(len == 1){
        const char c = word.at(0);
        return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
    }

    const char b = word.at(0),e = word.at(len - 1);
    if(b == '.' || b == '-' || e == '.' || e == '-'){
        return false;
    }

    bool hasAlphabet = false,hasAlphabetBeforeFirstDot = false,hasDot = false;
    char c;
    for(int  i = 0;i < len;++i){
        c = word.at(i);
        if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')){
            hasAlphabet = true;
            continue;
        }else if(c >= '0' && c <= '9'){
            continue;
        }else if(c == '.'){
            if(!hasDot){
                hasDot = true;
                if(hasAlphabet){
                    hasAlphabetBeforeFirstDot = true;
                }
            }
            continue;
        }else if(c == '_' || c == '-'){
            continue;
        }
        return false;
    }

    if(!hasAlphabet){
        return false;
    }
    if(hasDot && !hasAlphabetBeforeFirstDot){
        return false;
    }
    return true;
}

SqlJoinType SqlSyntaxUtils::getJoinType(const std::string& word){
    std::string myword = XStringUtils::toLowerCase(XStringUtils::trim(word));
    if(myword == "join"){
        return SqlJoinType::JOIN;
    }else if(myword == "inner"){
        return SqlJoinType::INNER;
    }else if(myword == "outer"){
        return SqlJoinType::OUTER;
    }else if(myword == "left"){
        return SqlJoinType::LEFT;
    }else if(myword == "right"){
        return SqlJoinType::RIGHT;
    }else if(myword == "full"){
        return SqlJoinType::FULL;
    }else{
        return SqlJoinType::NONE;
    }
}

SqlWindowType SqlSyntaxUtils::getWindowType(const std::string& word){
    std::string myword = XStringUtils::toLowerCase(XStringUtils::trim(word));
    if(myword == "pattern"){
        return SqlWindowType::PATTERN;
    }else if(myword == "sliding"){
        return SqlWindowType::SLIDING;
    }else if(myword == "tumbling"){
        return SqlWindowType::TUMBLING;
    }else{
        return SqlWindowType::NONE;
    } 
}