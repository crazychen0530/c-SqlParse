#ifndef EXPRESSION_MODEL_UTILS_H
#define EXPRESSION_MODEL_UTILS_H
#include <string>
#include <vector>
#include <memory>

// 前置声明 —— 全部由 libexd.so 提供
#include "Expression.h"
#include "CodeBlock.h"
#include "BinaryOperatorBlock.h"
#include "ElementBlock.h"
#include "AndControlBlock.h"
#include "MultiAndControlBlock.h"
#include "MultiNegAndConditionControlBlock.h"
#include "Operator.h"
#include "Contain.h"
#include "Equal.h"
#include "Exact.h"


class ExpressionModelUtils {
public:

    static bool isReduceJoinCondition(
        const std::shared_ptr<Expression>& exp,
        const std::string& leftAlias,
        const std::string& rightAlias,
        std::vector<std::string>& leftTerms,
        std::vector<std::string>& rightTerms);

    static std::string mergeTerms(const std::vector<std::string>& terms);

private:
    ExpressionModelUtils() = default;
    // 递归判断 CodeBlock 结构
    static bool isReduceJoinCodeBlock(
        const std::shared_ptr<CodeBlock>& block,
        const std::string& leftAlias,
        const std::string& rightAlias,
        std::vector<std::string>& leftTerms,
        std::vector<std::string>& rightTerms);
};

#endif