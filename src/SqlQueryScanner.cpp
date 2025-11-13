#include "SqlQueryScanner.h"
#include "SqlSyntaxUtils.h"
#include "XStringUtils.h"
#include "XNumberUtils.h"
#include <variant>
#include "../include/QueryRelation.h"
#include "../include/TableRelation.h"
#include "../include/IntervalSpec.h"
#include "../include/SqlFragment.h"
#include "../include/SqlJoinSpec.h"
#include "../include/SqlJoinType.h"
#include "../include/SqlRelation.h"
#include "../include/SqlStatement.h"
#include "../include/SqlWindowSpec.h"
#include "../include/SqlWindowType.h"
#include "../include/SqlSyntaxUtils.h"
#include "../include/PatternWindowSpec.h"
#include "../include/SlidingWindowSpec.h"
#include "../include/TumblingWindowSpec.h"
#include "../include/StatementParseException.h"
#include "../include/EndOfQueryException.h"
#include "../include/Protocol.h"
#include "../include/ProtocolRelation.h"
 
const std::unordered_set<std::string> SqlQueryScanner::BEGIN_WORDS = {"insert","select"};
const std::unordered_set<std::string> SqlQueryScanner::SELECT_WORDS = {"into","from"};
const std::unordered_set<std::string> SqlQueryScanner::FROM_WORDS = {"left","right","full","inner","outer","join","where","group","interval","window","session","limit"};

const std::unordered_set<std::string> SqlQueryScanner::WHERE_WORDS= {"group","interval","window","session","limit"};
const std::unordered_set<std::string> SqlQueryScanner::GROUP_BY_WORDS = {"having","limit"};
const std::unordered_set<std::string> SqlQueryScanner::INTERVAL_WORDS = {"every"};
const std::unordered_set<std::string> SqlQueryScanner::INTERVAL_BY_WORDS = GROUP_BY_WORDS;
const std::unordered_set<std::string> SqlQueryScanner::TIME_UNITS = {"millisecond","second","minute","hour"};
const std::unordered_set<std::string> SqlQueryScanner::HAVING_WORDS = {"limit"};

const std::unordered_set<std::string> SqlQueryScanner::OUTER_WORDS = {"outer","join"};
const std::unordered_set<std::string> SqlQueryScanner::JOIN_WORDS = {"on"};
const std::unordered_set<std::string> SqlQueryScanner::JOIN_ON_WORDS = {"where","group","interval","window","session","limit"};

const std::unordered_set<std::string> SqlQueryScanner::WINDOW_KINDS = {"pattern","sliding","tumbling"};
const std::unordered_set<std::string> SqlQueryScanner::PATTERN_ON_WORDS = {"until"};
const std::unordered_set<std::string> SqlQueryScanner::WINDOW_ON_WORDS = {"partition","having"};
const std::unordered_set<std::string> SqlQueryScanner::WINDOW_UNTIL_WORDS = {"partition","having"};
const std::unordered_set<std::string> SqlQueryScanner::WINDOW_PARTITION_BY_WORDS  = {"order"};
const std::unordered_set<std::string> SqlQueryScanner::WINDOW_ORDER_BY_WORDS = {"having"};
const std::unordered_set<std::string> SqlQueryScanner::WINDOW_ENDS = {"having",")"};

SqlQueryScanner::SqlQueryScanner(const std::string& query): SqlQueryScanner(query,0){}

SqlQueryScanner::SqlQueryScanner(const std::string& query, const int& pos) {
    this->query = query.empty() ? "" : query;
    this->length = static_cast<int>(this->query.length());
    this->begin = this->pos = pos >= 0 ? pos : 0;
    this->insubquery = (this->begin != 0);
}

int SqlQueryScanner::getBeginPosition(){
    return this->begin;
}

int SqlQueryScanner::getCurrentPosition(){
    return this->pos;
}

std::string SqlQueryScanner::getRawQueryString(){
    return this->query;
}

SqlQueryScanner& SqlQueryScanner::setQuoteChar(const char quoteChar){
    this->quoteChar = quoteChar;
    return *this;
}

SqlQueryScanner& SqlQueryScanner::setTerminateChar(const char terminateChar){
    this->terminateChar = terminateChar;
    return *this;
}

int SqlQueryScanner::getSubqueryEndPosition(){
    return subqueryParentheseEnd;
}

bool SqlQueryScanner::isTerminated(){
    return this->terminated || this->pos >= this->length || this->subqueryParentheseEnd > 0;
}

std::shared_ptr<SqlStatement> SqlQueryScanner::scan(){
    std::shared_ptr<SqlStatement> stmt = std::make_shared<SqlStatement>();
    std::string word;

    word = readOneOfWords(BEGIN_WORDS,"");
    if("insert" == XStringUtils::toLowerCase(word)){
        if(insubquery){
            throw StatementParseException("SQL_SYNTAX_INSERT_NOT_ALLOWED_IN_SUBQUERY");
        }
        readOneWord("into","insert");

        const std::string field = readOneField("table","insert_into");
        const std::shared_ptr<ProtocolRelation> prel = std::make_shared<ProtocolRelation>(ProtocolUtils::get(field));
        if(prel->getProtocol() != Protocol::NONE){
            stmt->setInto(prel);
        }else{
            const std::shared_ptr<TableRelation> relation = std::make_shared<TableRelation>();
            relation->setName(field);
            if(relation->isSystemTable()){
                throw StatementParseException("SQL_SYNTAX_INSERT_SYSTEM_TABLES_NOT_ALLOWED");
            }
            stmt->setInto(relation);
        }
        hasinto = true;

        readOneWord("select","insert_into");
    }

    stmt->setSelects(readExprs(SELECT_WORDS,SqlFragment::SELECT));
    if(isTerminated()){
        return finalize(stmt,shared_from_this());
    }

    word = readOneOfWords(SELECT_WORDS,"select");
    if("into" == XStringUtils::toLowerCase(word)){
        if(insubquery){
            throw StatementParseException("SQL_SYNTAX_INTO_NOT_ALLOWED_IN_SUBQUERY");
        }
        if(hasinto){
            throw StatementParseException("SQL_SYNTAX_INTO_ALREADY_DEFINED");
        }

        const std::string field = readOneField("table","into");
        const std::shared_ptr<ProtocolRelation> prel = std::make_shared<ProtocolRelation>(ProtocolUtils::get(field));
        if(prel->getProtocol() != Protocol::NONE){
            stmt->setInto(prel);
        }else{
            const std::shared_ptr<TableRelation> relation = std::make_shared<TableRelation>();
            relation->setName(field);
            if(relation->isSystemTable()){
                throw StatementParseException("SQL_SYNTAX_INSERT_NOT_ALLOWED_WITH_SYSTEM_TABLES");
            }
            stmt->setInto(relation);
        }
        hasinto = true;

        word = readOneWord("from","into");
    }

    std::shared_ptr<SqlRelation> fromRel = readRelation(FROM_WORDS,SqlFragment::FROM);
    stmt->setFrom(fromRel);
    if(isTerminated()){
        return finalize(stmt,shared_from_this());
    }

    SqlJoinType joinType;
    word = readOneOfWords(FROM_WORDS,"from");
    joinType = SqlSyntaxUtils::getJoinType(word);
    if(joinType != SqlJoinType::NONE){
        switch(joinType){
            case SqlJoinType::JOIN:
                joinType = SqlJoinType::INNER;
                break;
            case SqlJoinType::INNER:
                readOneWord("join","inner");
                break;
            case SqlJoinType::OUTER:
                readOneWord("join","outer");
                joinType = SqlJoinType::FULL;
                break;
            case SqlJoinType::LEFT:
            case SqlJoinType::RIGHT:
            case SqlJoinType::FULL:
                if("outer" == XStringUtils::toLowerCase(readOneOfWords(OUTER_WORDS,toString(joinType)))){
                    readOneWord("join",toString(joinType)+ "_OUTER");
                }
                break;
            default:{}
        }
        std::shared_ptr<SqlRelation> joinRel = readRelation(JOIN_WORDS,SqlFragment::JOIN);
        readOneWord("on","join");

        if(stmt->getSelectExpList() != nullptr){
            stmt->getSelectExpList()->setLeftRightAlias(stmt->getFrom()->getAlias(),joinRel->getAlias());

        }

        std::shared_ptr<SqlJoinSpec> joinSpec = std::make_shared<SqlJoinSpec>();
        joinSpec->setRelation(joinRel);
        joinSpec->setType(joinType);
        joinSpec->setCondition(readExprs(JOIN_ON_WORDS,SqlFragment::JOIN_ON));
        joinSpec->getConditionExp()->setLeftRightAlias(stmt->getFrom()->getAlias(),joinRel->getAlias());
        stmt->setJoin(joinSpec);

        if(isTerminated()){
            return finalize(stmt,shared_from_this());
        }

        throw StatementParseException(std::string("SQL_SYNTAX_WORDS_AFTER_JOIN_NOT_YET_SUPPORTED: ") + readWord());
    }
    if("where" == XStringUtils::toLowerCase(word)){
        stmt->setWhere(readExprs(WHERE_WORDS,SqlFragment::WHERE));
        if(isTerminated()){
            return finalize(stmt,shared_from_this());
        }
        word = readOneOfWords(WHERE_WORDS,"where");
    }

    if("group" == XStringUtils::toLowerCase(word) || "interval" == XStringUtils::toLowerCase(word)){
        const std::string bykind = word;
        readOneWord("by",word);

        if("group" == XStringUtils::toLowerCase(bykind)){
            stmt->setGroupbys(readExprs(GROUP_BY_WORDS,SqlFragment::GROUP_BY));
            if(isTerminated()){
                return finalize(stmt,shared_from_this());
            }
            word = readOneOfWords(GROUP_BY_WORDS,"group_by");
        }else{
            const std::shared_ptr<IntervalSpec> ivspec = std::make_shared<IntervalSpec>();
            ivspec->setInterval(readExprs(INTERVAL_WORDS,SqlFragment::INTERVAL_BY));

            readOneWord("every","interval");
            ivspec->setTimeAmount(readOnePosInt("every"));
            ivspec->setTimeUnit(readOneOfWords(TIME_UNITS,"every"));
            stmt->setInterval(ivspec);

            if(isTerminated()){
                return finalize(stmt,shared_from_this());
            }
            word = readOneOfWords(INTERVAL_BY_WORDS,"interval_by");
        }

        if("having" == XStringUtils::toLowerCase(word)){
            if(isTerminated()){
                throw StatementParseException("SQL_SYNTAX_INVALID_HAVING_SYNTAX");
            }

            stmt->setHaving(readExprs(HAVING_WORDS,SqlFragment::HAVING));
            if(isTerminated()){
                return finalize(stmt,shared_from_this());
            }
            word = readOneOfWords(HAVING_WORDS,"having");
        }
    }else if("window" == XStringUtils::toLowerCase(word) || "session" == XStringUtils::toLowerCase(word)){
        inspec = true;
        specParentheseEnd = -1;

        std::string windowKind = XStringUtils::toUpperCase(word);
        SqlWindowType windowType;

        readOneWord("over",windowKind);
        readOneWord("(",windowKind + "_over");
        windowType = SqlSyntaxUtils::getWindowType(readOneOfWords(WINDOW_KINDS,windowKind + "_over"));
        readOneWord("on",windowKind + "_over_" + toString(windowType));

        std::shared_ptr<SqlWindowSpec> spec  = nullptr;
        if(windowType == SqlWindowType::PATTERN){
            std::string windowEnter = readExprs(PATTERN_ON_WORDS,SqlFragment::WINDOW_ON);
            readOneWord("until",windowKind + "_over_pattern");

            std::string windowExit = readExprs(WINDOW_UNTIL_WORDS,SqlFragment::WINDOW_UNTIL);
            if(isTerminated()){
                throw StatementParseException(std::string("SQL_SYNTAX_INVALID_")+ windowKind + "_SYNTAX" + word);
            }

            const std::shared_ptr<PatternWindowSpec> pwspec = std::make_shared<PatternWindowSpec>();
            pwspec->setKind(windowKind);
            pwspec->setEnter(windowEnter);
            pwspec->setExit(windowExit);
            spec = pwspec;
        }else{
            std::string inclusion = readExprs(WINDOW_ON_WORDS,SqlFragment::WINDOW_ON);
            if(isTerminated()){
                throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + windowKind + "_SYNTAX" + word);
            }
            if(windowType == SqlWindowType::SLIDING){
                const std::shared_ptr<SlidingWindowSpec> swspec = std::make_shared<SlidingWindowSpec>();
                swspec->setKind(windowKind);
                swspec->setInclusion(inclusion);
                spec  = swspec;
            }else{
                const std::shared_ptr<TumblingWindowSpec> twspec = std::make_shared<TumblingWindowSpec>();
                twspec->setKind(windowKind);
                twspec->setInclusion(inclusion);
                spec = twspec;
            }
        }

        word = readOneOfWords(WINDOW_ON_WORDS,windowKind);
        if("partition" == XStringUtils::toLowerCase(word)){
            readOneWord("by",windowKind + "_partition");
            spec->setKeys(readExprs(WINDOW_PARTITION_BY_WORDS,SqlFragment::PARTITION_BY));

            readOneWord("order",windowKind + "_partition");
            readOneWord("by",windowKind + "_partition_order");
            spec->setSorts(readExprs(WINDOW_ORDER_BY_WORDS,SqlFragment::ORDER_BY));

            word = readOneOfWords(WINDOW_ENDS,windowKind + "_partition_order");
            if("having" == XStringUtils::toLowerCase(word)){
                spec->setHaving(readExprs({},SqlFragment::HAVING));
                readOneWord(")",windowKind + "_partition_order_having");
            }
        }else if("having" == XStringUtils::toLowerCase(word)){
            spec->setHaving(readExprs({},SqlFragment::HAVING));
            readOneWord(")",windowKind + "_having");
        }
        stmt->setWindow(spec);

        if(unwrapSpecAndCheckFinalize()){
            return finalize(stmt,shared_from_this());
        }
        word = readOneWord("limit",windowKind);
    }

    if(insubquery){
        throw StatementParseException("SQL_SYNTAX_LIMIT_NOT_ALLOWED_IN_SUBQUERY");
    }
    stmt->setLimit(readOnePosInt("limit"));

    if(!isTerminated()){
        throw StatementParseException("SQL_SYNTAX_INVALID_CONTENT_AFTER_LIMIT");
    }

    return finalize(stmt,shared_from_this());
}

std::shared_ptr<SqlStatement> SqlQueryScanner::finalize(std::shared_ptr<SqlStatement> stmt,std::shared_ptr<SqlQueryScanner> scanner){
    const int endPosition = scanner->getSubqueryEndPosition() > 0 ? scanner->getSubqueryEndPosition() : scanner->getCurrentPosition();
    const std::string query  = XStringUtils::trim(scanner->getRawQueryString().substr(scanner->getBeginPosition(),endPosition- scanner->getBeginPosition()));
    stmt->setQuery(query);
    return stmt;
}

bool SqlQueryScanner::unwrapSpecAndCheckFinalize(){
    inspec = false;
    if(insubquery){
        if(isTerminated() || !(")" == XStringUtils::toLowerCase(readWord()))){
            throw StatementParseException("SQL_SYNTAX_UNEXPECTED_END_OF_SUBQUERY");
        }
        pos--;

        return true;
    }

    if(isTerminated()){
        return true;
    }
    return false;
}

std::string SqlQueryScanner::readWord(){
    const int eidx = peek(query,pos,SqlFragment::NONE);
    if(eidx < 0){
        throw EndOfQueryException("SQL_SYNTAX_UNEXPECTED_END_OF_QUERY");
    }

    const std::string word = XStringUtils::trim(query.substr(pos,eidx + 1 - pos));
    pos = eidx + 1;

    return word;
}



SqlQueryScanner::ReadResult SqlQueryScanner::read(std::unordered_set<std::string> stopKeywords,SqlFragment fragment){
    if (fragment == SqlFragment::FROM || fragment == SqlFragment::JOIN) { //check for sub-query
        const int qidx = peek(query, pos, fragment);
        if (qidx > 0) {
            const std::string word = XStringUtils::trim(query.substr(pos, qidx + 1 - pos));
            if ("(" == word) {
                std::shared_ptr<SqlQueryScanner> subqueryScanner = std::make_shared<SqlQueryScanner>(query, qidx +1);
                std::shared_ptr<SqlStatement> stmt = subqueryScanner->scan();
                pos = subqueryScanner->getCurrentPosition() + 1;

                std::shared_ptr<QueryRelation> relation = std::make_shared<QueryRelation>();
                relation->setStatement(stmt);
                
                std::string alias = "", pclose = "";
                int aidx = peek(query, pos, fragment); //alias
                if (aidx > 0) {
                    alias = XStringUtils::trim(query.substr(pos, aidx + 1- pos));
                    if (")" == alias) {
                        if (!insubquery) {
                            throw StatementParseException("SQL_SYNTAX_TOO_MANY_SUBQUERY_PARENTHESE_CLOSE");
                        }
                        subqueryParentheseEnd = aidx; //subquery need to set the end ) position
                    } else if (!SqlSyntaxUtils::isReservedKeyword(alias)) { //reserved keyword means no alias
                        if (!SqlSyntaxUtils::isValidNameOrAlias(alias)) {
                            throw StatementParseException(std::string("SQL_SYNTAX_INVALID_SUBQUERY_ALIAS_CHARACTER: ") + alias);
                        }
                        relation->setAlias(alias);
                        pos = aidx + 1;
                        
                        if (insubquery) {
                            aidx = peek(query, pos, fragment); //look for )
                            pclose = XStringUtils::trim(query.substr(pos, aidx + 1 - pos));
                            if (")" != pclose) {
                                throw StatementParseException(std::string("SQL_SYNTAX_SUBQUERY_MISSING_PARENTHESE_AFTER: ") + alias);
                            }
                            subqueryParentheseEnd = aidx; //subquery need to set the end ) position
                        }
                    } else if (insubquery) {
                        throw StatementParseException(std::string("SQL_SYNTAX_SUBQUERY_ALIAS_USING_RESERVED_WORD: ") + alias);
                    }
                }
                
                return relation;
            }
        }
    }
    
    int parentheseNestDepth = 0, eidx = length - 1;
    bool quoted = false;
    std::string word = "";
    char c, v;
    
    for (int i = pos; i < length; ++i) {
        c = query.at(i);
        if (c == quoteChar) {
            if (quoted) {
                if (i+1 < length && query.at(i+1) == quoteChar) {
                    ++i; //double quote escape, not yet unquote
                } else {
                    quoted = false; // ready to unquote
                }
            } else {
                quoted = true;
            }
        } else if (quoted) {
            continue;
        } else if (c == terminateChar) {
            terminated = true;
            eidx = i - 1;
            break;
        } else if (c == ESCAPE) {
            ++i;
        } else {
            if (SqlSyntaxUtils::isWhiteSpace(c)) {
                const int widx = peek(query, i + 1, fragment);
                if (widx < 0) {
                    if (SqlSyntaxUtils::canExitOnEnd(fragment)) {
                        break;
                    }
                    throw StatementParseException(std::string("SQL_SYNTAX_UNEXPECTED_END") + keywordsToErrorMessage(stopKeywords));
                }
                
                word = XStringUtils::toLowerCase(XStringUtils::trim(query.substr(i + 1, widx - i)));
                
                v = word.at(0);
                if (v == PARENTHESE_CLOSE && (inspec || insubquery)) {
                    if (parentheseNestDepth != 0) {
                        throw StatementParseException("SQL_SYNTAX_UNEXPECTED_PARENTHESE_CLOSE");
                    }
                    if (inspec) {
                        specParentheseEnd = widx;
                    } else if (insubquery) {
                        subqueryParentheseEnd = widx;
                    } 
                    eidx = widx - 1;
                    break;
                } else if (v == terminateChar) {
                    terminated = true;
                    eidx = widx - 1;
                    break;
                } else if (v != ESCAPE && SqlSyntaxUtils::isReservedKeyword(word)) {
                    if (!stopKeywords.empty() && stopKeywords.count(word) && parentheseNestDepth == 0) {
                        eidx = i - 1;
                        break;
                    } else {
                        throw StatementParseException("SQL_SYNTAX_INVALID_KEYWORD_PLACEMENT: " + XStringUtils::toUpperCase(word));
                    }
                }
                
                i = widx;
            } else if (c == PARENTHESE_OPEN) {
                parentheseNestDepth++;
            } else if (c == PARENTHESE_CLOSE) {
                if (parentheseNestDepth > 0) {
                    parentheseNestDepth--;
                } else if (inspec) {
                    specParentheseEnd = i;
                    eidx = i - 1;
                    break;
                } else if (insubquery) {
                    subqueryParentheseEnd = i;
                    eidx = i - 1;
                    break;
                } else {
                    throw StatementParseException("SQL_SYNTAX_UNEXPECTED_PARENTHESE_CLOSE");
                }
            }
        }
    }
    
    if (quoted) {
        throw StatementParseException("SQL_SYNTAX_UNCLOSED_QUOTES_AFTER_" + toString(fragment));
    }
    if (parentheseNestDepth > 0) {
        throw StatementParseException("SQL_SYNTAX_UNCLOSED_PARENTHESE_AFTER_" + toString(fragment));
    }
    
    std::string exp = XStringUtils::trim(query.substr(pos, eidx + 1 - pos));
    if (exp.length() == 0) {
        throw StatementParseException("SQL_SYNTAX_MISSING_EXPRESSIONS_AFTER_" + toString(fragment));
    }
    if (exp.at(exp.length() - 1) == COMMA) {
        throw StatementParseException("SQL_SYNTAX_TOO_MANY_COMMA_AFTER_" + toString(fragment));
    }
    if (terminated || eidx + 1 == length) {
        if (!SqlSyntaxUtils::canExitOnEnd(fragment)) {
            throw StatementParseException("SQL_SYNTAX_UNEXPECTED_END" + keywordsToErrorMessage(stopKeywords));
        }
        if (terminated && eidx + 2 != length) {
            throw StatementParseException("SQL_SYNTAX_UNEXPECTED_CONTENT_AFTER_END");
        }
    } else if ((specParentheseEnd < 0 && subqueryParentheseEnd < 0) && !word.empty() &&word.at(0) != quoteChar) { //parsed at least a valid word
        if (!stopKeywords.empty() && stopKeywords.size() > 0 && !stopKeywords.count(word)) {
            throw StatementParseException("SQL_SYNTAX_UNEXPECTED_END" + keywordsToErrorMessage(stopKeywords));
        }
    }

    //set the begin, now onto valid fragments
    pos = eidx + 1;
    
    if ("*" == exp) {
        if (fragment != SqlFragment::SELECT) {
            throw EngineException("SQL_SYNTAX_INVALID_STAR_WITH_" + toString(fragment));
        }
    } else if (fragment == SqlFragment::FROM || fragment == SqlFragment::JOIN){
        std::shared_ptr<TableRelation> relation = std::make_shared<TableRelation>();
        const int idx = exp.find(' ');
        if (idx > 0) { //has alias
            const std::string name = XStringUtils::trim(exp.substr(0, idx));
            const std::string alias = XStringUtils::trim(exp.substr(idx + 1));
            if (!SqlSyntaxUtils::isValidNameOrAlias(name)) {
                throw StatementParseException("SQL_SYNTAX_INVALID_FROM_NAME_CHARACTER: " + name);
            }
            if (alias.length() > 0 && !SqlSyntaxUtils::isValidNameOrAlias(alias)) {
                throw StatementParseException("SQL_SYNTAX_INVALID_FROM_ALIAS_CHARACTER: " + alias);
            }
            relation->setName(name);
            relation->setAlias(alias);
        } else {
            if (!SqlSyntaxUtils::isValidNameOrAlias(exp)) {
                throw StatementParseException("SQL_SYNTAX_INVALID_FROM_NAME: " + exp);
            }
            relation->setName(exp);
        }
        return relation;
    }
    
    return exp;
}





int SqlQueryScanner::peek(const std::string& query,const int bpos,const SqlFragment fragment){
    const int length = query.length();
    if(bpos >= length){
        return -1;
    }

    char c = query.at(bpos);
    int begin = bpos,end = -1;

    if(SqlSyntaxUtils::isWhiteSpace(c)){
        begin = -1;

        for(int i = bpos + 1;i < length;++i){
            if(SqlSyntaxUtils::isWhiteSpace(query.at(i))){
                continue;
            }else{
                begin = i;
                break;
            }
        }
        if(begin < 0){
            return -1;
        }
    }

    if(query.at(begin) == '\''){
        for(int i = begin + 1; i <length;++i){
            c = query.at(i);
            if(c == '\''){
                if(i + 1 == length){
                    return i;
                }
                if(query.at(i + 1) != '\''){
                    return i;
                }
                ++i; 
            }
        }
        throw StatementParseException(std::string("SQL_SYNTAX_UNCLOSED_QUOTES") + (fragment != SqlFragment::NONE ? "_AFTER_" + toString(fragment) : ""));
    }else{
        for(int i = begin;i < length;++i){
            c = query.at(i);
            if ((c >= 'a' && c <= 'z') 
                    || (c >= 'A' && c <= 'Z') 
                    || (c >= '0' && c <= '9') 
                    || c == '_' 
                    || c == '-'
                    || c == '.'){
                continue;
            }
            if(i == begin){
                return i;
            }
            end = i -1;
            break;
        }
    }
    return end < 0 ? length - 1: end;
}

std::string SqlQueryScanner::readOneWord(const std::string keyword,const std::string syntax){
    if(isTerminated()){
        if(XStringUtils::isBlank(syntax)){
            throw StatementParseException(std::string("SQL_SYNTAX_MISSING_") + XStringUtils::toUpperCase(keyword));
        }
        throw StatementParseException(std::string("SQL_SYNTAX_MISSING_") + XStringUtils::toUpperCase(keyword) + "_AFTER_" + XStringUtils::toUpperCase(syntax));
    }

    const std::string w = readWord();
    if(!(XStringUtils::toLowerCase(w) == XStringUtils::toLowerCase(keyword))){
        if(XStringUtils::isBlank(syntax)){
            throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(keyword) + "_SYNTAX: " + w);
        }
        throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(syntax) + "_" + XStringUtils::toUpperCase(keyword) + "_SYNTAX: " + w);
    }

    return w;
}

std::string SqlQueryScanner::readOneOfWords(const unordered_set<std::string> keywords,const std::string syntax){
    if (isTerminated()) {
        std::string msg;
        bool first = true;
        for (const auto& kw : keywords) {
            if (!first) {
                msg += "_or_";
            }
            msg += kw;
            first = false;
        }

        if (XStringUtils::isBlank(syntax)) {
            throw StatementParseException(std::string("SQL_SYNTAX_MISSING_") + XStringUtils::toUpperCase(msg));
        }
        throw StatementParseException(std::string("SQL_SYNTAX_MISSING_") + XStringUtils::toUpperCase(msg) + "_AFTER_" + XStringUtils::toUpperCase(syntax));
    }

    std::string w = readWord();
    std::string lowerW = XStringUtils::toLowerCase(w);

    if (keywords.find(lowerW) == keywords.end()) {
        if (XStringUtils::isBlank(syntax)) {
            throw StatementParseException(std::string("SQL_SYNTAX_INVALID_SQL_SYNTAX: ") + w);
        }
        throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(syntax) + "_SYNTAX: " + w);
    }

    return w;

}


std::string SqlQueryScanner::readOneField(const std::string nameOfField,const std::string syntax){
    if (isTerminated()) {
        throw StatementParseException(std::string("SQL_SYNTAX_MISSING_") + XStringUtils::toUpperCase(nameOfField) + "_AFTER_" + XStringUtils::toUpperCase(syntax));
    }
    
    const std::string w = XStringUtils::trim(readWord());
    if (!SqlSyntaxUtils::isValidNameOrAlias(w)) {
        throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(syntax) + "_SYNTAX: " + w);
    }
    
    return w;
}

int SqlQueryScanner::readOnePosInt(const std::string syntax){
    if (isTerminated()) {
            throw StatementParseException(std::string("SQL_SYNTAX_MISSING_NUMBER_AFTER_") + XStringUtils::toUpperCase(syntax));
        }
        std::string w = readWord();
        if (!XNumberUtils::isDigits(w)) {
            throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(syntax) + "_NUMBER: " + w);
        }
        try {
            long x = std::stol(w);
            if (x <= 0 || x > std::numeric_limits<int>::max()) {
                throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(syntax) + "_NUMBER: " + w);
            }
            return (int) x;
        } catch (const std::invalid_argument& e) {
            throw StatementParseException(std::string("SQL_SYNTAX_INVALID_") + XStringUtils::toUpperCase(syntax) + "_NUMBER: " + w);
        } catch (const std::out_of_range& e ){
            throw StatementParseException("SQL_SYNTAX_INVALID_" + XStringUtils::toUpperCase(syntax) + "_NUMBER: " + w);
        }
}

std::string SqlQueryScanner::readExprs(const unordered_set<std::string> wordsToStopAt,const SqlFragment syntax){
    if (isTerminated()) {
        throw StatementParseException(std::string("SQL_SYNTAX_MISSING_EXPRESSIONS_AFTER_") + XStringUtils::toUpperCase(toString(syntax)));
    }
    
    auto result = read(wordsToStopAt,syntax);

    if(!std::holds_alternative<std::string>(result)){//检查variant是否持有string类型
        throw StatementParseException("SQL_SYNTAX_INTERNAL_TYPE_ERROR_EXPECT_STRING");
    }

    return (XStringUtils::trim(std::get<std::string>(result)));
}

std::shared_ptr<SqlRelation> SqlQueryScanner::readRelation(const unordered_set<std::string> wordsToStopAt,const SqlFragment syntax){
    if (isTerminated()) {
        throw StatementParseException(std::string("SQL_SYNTAX_MISSING_RELATION_AFTER_") + XStringUtils::toUpperCase(toString(syntax)));
    }
        
    return std::get<std::shared_ptr<SqlRelation>>(read(wordsToStopAt,syntax));
}

std::string SqlQueryScanner::keywordsToErrorMessage(std::unordered_set<std::string> words){
    if (words.empty() || words.size() == 0) {
        return "";
    }
    std::string msg;
    auto iter = words.begin();
    if (iter != words.end()) {
        msg = "_WITHOUT_" + XStringUtils::toUpperCase(*iter);  // 第一个元素
        ++iter;
        while (iter != words.end()) {
            msg += "_OR_" + XStringUtils::toUpperCase(*iter);  // 后续元素
            ++iter;
        }
    }
    return msg;
}

