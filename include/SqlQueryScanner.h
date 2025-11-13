#ifndef SQL_QUERY_SCANNER_H
#define SQL_QUERY_SCANNER_H

#include <unordered_set>
#include <string>
#include <memory>
#include <variant>
#include "SqlStatement.h"
#include "EngineException.h"
#include "SqlFragment.h"

class SqlQueryScanner : public std::enable_shared_from_this<SqlQueryScanner>{
public:
    SqlQueryScanner(const std::string& query);
    SqlQueryScanner(const std::string& query,const int& pos);
    
    using ReadResult = std::variant<std::shared_ptr<SqlRelation>,std::string>;

    int getBeginPosition();

    int getCurrentPosition();

    std::string getRawQueryString();

    SqlQueryScanner& setQuoteChar(const char quoteChar);

    SqlQueryScanner& setTerminateChar(const char terminateChar);

    int getSubqueryEndPosition();

    bool isTerminated();

    std::shared_ptr<SqlStatement> scan();

protected:
    std::shared_ptr<SqlStatement> finalize(std::shared_ptr<SqlStatement> stmt,std::shared_ptr<SqlQueryScanner> scanner);

    
private:
    static const char PARENTHESE_OPEN = '(';
    static const char PARENTHESE_CLOSE = ')';
    static const char COMMA = ',';
    static const char ESCAPE = '\\';

    static const std::unordered_set<std::string> BEGIN_WORDS;
    static const std::unordered_set<std::string> SELECT_WORDS;
    static const std::unordered_set<std::string> FROM_WORDS;

    static const std::unordered_set<std::string> WHERE_WORDS;
    static const std::unordered_set<std::string> GROUP_BY_WORDS;
    static const std::unordered_set<std::string> INTERVAL_WORDS;
    static const std::unordered_set<std::string> INTERVAL_BY_WORDS;
    static const std::unordered_set<std::string> TIME_UNITS;
    static const std::unordered_set<std::string> HAVING_WORDS;

    static const std::unordered_set<std::string> OUTER_WORDS;
    static const std::unordered_set<std::string> JOIN_WORDS;
    static const std::unordered_set<std::string> JOIN_ON_WORDS;

    static const std::unordered_set<std::string> WINDOW_KINDS;
    static const std::unordered_set<std::string> PATTERN_ON_WORDS;
    static const std::unordered_set<std::string> WINDOW_ON_WORDS;
    static const std::unordered_set<std::string> WINDOW_UNTIL_WORDS;
    static const std::unordered_set<std::string> WINDOW_PARTITION_BY_WORDS;
    static const std::unordered_set<std::string> WINDOW_ORDER_BY_WORDS;
    static const std::unordered_set<std::string> WINDOW_ENDS;

    std::string query = "";
    int length = 0;
    int begin = 0;
    int pos = 0;
    int subqueryParentheseEnd = -1;
    int specParentheseEnd = -1;
    bool hasinto = false;
    bool insubquery = false;
    bool inspec = false;
    bool terminated =false;

    char quoteChar = '\'';
    char terminateChar = ';';

    bool unwrapSpecAndCheckFinalize();

    std::string readWord();

    ReadResult read(std::unordered_set<std::string> stopKeywords,SqlFragment fragment);
    
    static int peek(const std::string& query,const int bpos,const SqlFragment fragment);

    std::string readOneWord(const std::string keyword,const string syntax);

    std::string readOneOfWords(const unordered_set<std::string> keywords,const std::string syntax);

    std::string readOneField(const std::string nameOfField,const std::string syntax);

    int readOnePosInt(const std::string syntax);

    std::string readExprs(const unordered_set<std::string> wordsToStopAt,const SqlFragment syntax);

    std::shared_ptr<SqlRelation> readRelation(const unordered_set<std::string> wordsToStopAt,const SqlFragment syntax);

    static std::string keywordsToErrorMessage(std::unordered_set<std::string> words);
};
#endif