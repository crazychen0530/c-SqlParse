#include "ExpressionModelUtils.h"
#include <typeinfo>

bool ExpressionModelUtils::isReduceJoinCondition(
    const std::shared_ptr<Expression>& exp,
    const std::string& leftAlias,
    const std::string& rightAlias,
    std::vector<std::string>& leftTerms,
    std::vector<std::string>& rightTerms)
{
    return isReduceJoinCodeBlock(exp->getBlock(), leftAlias, rightAlias, leftTerms, rightTerms);
}

bool ExpressionModelUtils::isReduceJoinCodeBlock(
    const std::shared_ptr<CodeBlock>& block,
    const std::string& leftAlias,
    const std::string& rightAlias,
    std::vector<std::string>& leftTerms,
    std::vector<std::string>& rightTerms)
{

    // BinaryOperatorBlock 情况
    if (auto bob = std::dynamic_pointer_cast<BinaryOperatorBlock>(block)) {
        auto op = bob->getOperator();
        // 运算符必须是 Contain、Equal 或 Exact
        if (!(dynamic_cast<Contain*>(op.get()) ||
              dynamic_cast<Equal*>(op.get()) ||
              dynamic_cast<Exact*>(op.get()))) {
            return false;
        }

        auto sblocks = block->getSubBlocks();
        if (sblocks.size() < 2) return false;
        auto eb0 = std::dynamic_pointer_cast<ElementBlock>(sblocks[0]);
        auto eb1 = std::dynamic_pointer_cast<ElementBlock>(sblocks[1]);
        if (!eb0 || !eb1) return false;

        const std::string& n0 = eb0->getName();
        const std::string& n1 = eb1->getName();

        if (n0.find(leftAlias + ".") == 0) {
            leftTerms.push_back(n0.substr(leftAlias.size() + 1));
            if (n1.find(rightAlias + ".") != 0) return false;
            rightTerms.push_back(n1.substr(rightAlias.size() + 1));
        } else if (n0.find(rightAlias + ".") == 0) {
            rightTerms.push_back(n0.substr(rightAlias.size() + 1));
            if (n1.find(leftAlias + ".") != 0) return false;
            leftTerms.push_back(n1.substr(leftAlias.size() + 1));
        } else {
            return false;
        }
        return true;
    }

    // AndControlBlock
    if (auto acb = std::dynamic_pointer_cast<AndControlBlock>(block)) {
        for (auto& sub : acb->getSubBlocks()) {
            if (!isReduceJoinCodeBlock(sub, leftAlias, rightAlias, leftTerms, rightTerms))
                return false;
        }
        return true;
    }

    // MultiAndControlBlock
    if (auto macb = std::dynamic_pointer_cast<MultiAndControlBlock>(block)) {
        for (auto& sub : macb->getSubBlocks()) {
            if (!isReduceJoinCodeBlock(sub, leftAlias, rightAlias, leftTerms, rightTerms))
                return false;
        }
        return true;
    }

    // MultiNegAndConditionControlBlock
    if (auto mncb = std::dynamic_pointer_cast<MultiNegAndConditionControlBlock>(block)) {
        auto subs = mncb->getSubBlocks();
        for (size_t i = 0; i + 1 < subs.size(); i += 2) {
            if (!isReduceJoinCodeBlock(subs[i], leftAlias, rightAlias, leftTerms, rightTerms))
                return false;
        }
        return true;
    }

    return false;
}

std::string ExpressionModelUtils::mergeTerms(const std::vector<std::string>& terms)
{
    std::string merged = terms.at(0);
    for (size_t i = 1; i < terms.size(); ++i) {
        merged += "," + terms.at(i);
    }
    return merged;
}
