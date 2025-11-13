#ifndef SQL_JOIN_TYPE_H
#define SQL_JOIN_TYPE_H

#include <string>
enum class SqlJoinType{
    NONE,
    JOIN,
    INNER,
    LEFT,
    RIGHT,
    OUTER,
    FULL
};

inline std::string toString(SqlJoinType type) {
    switch (type) {
        case SqlJoinType::NONE:  return "NONE";
        case SqlJoinType::JOIN:  return "JOIN";
        case SqlJoinType::INNER: return "INNER";
        case SqlJoinType::LEFT:  return "LEFT";
        case SqlJoinType::RIGHT: return "RIGHT";
        case SqlJoinType::OUTER: return "OUTER";
        case SqlJoinType::FULL:  return "FULL";
        default:                 return "UNKNOWN";
    }
}

#endif