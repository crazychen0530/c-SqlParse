#include "../include/SqlQueryParser.h"
#include "../include/SqlQueryScanner.h"
#include "XStringUtils.h"   // 来自 libexd.so
#include "../include/SqlStatement.h"

std::shared_ptr<SqlStatement> SqlQueryParser::parse(const std::string& query) {
    std::string str = XStringUtils::removeCodeComments(query);
    if (XStringUtils::isBlank(str)) {
        return nullptr;
    }
    std::shared_ptr<SqlQueryScanner> scanner =  std::make_shared<SqlQueryScanner>(XStringUtils::trim(str));
    return scanner->scan();
}
