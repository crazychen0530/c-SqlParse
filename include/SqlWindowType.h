#ifndef SQL_WINDOW_TYPE_H
#define SQL_WINDOW_TYPE_H

#include<string>

enum class SqlWindowType{
    NONE,
    PATTERN,
    SLIDING,
    TUMBLING
};

inline std::string toString(SqlWindowType type) {
    switch (type) {
        case SqlWindowType::NONE:     return "NONE";
        case SqlWindowType::PATTERN:  return "PATTERN";
        case SqlWindowType::SLIDING:  return "SLIDING";
        case SqlWindowType::TUMBLING: return "TUMBLING";
        default:                      return "UNKNOWN";
    }
}

#endif