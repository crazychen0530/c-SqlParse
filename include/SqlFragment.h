#ifndef SQL_FRAGMENT_H
#define SQL_FRAGMENT_H

#include <string>
enum class SqlFragment{
    NONE,
    FROM,
    GROUP_BY,
    HAVING,
    INSERT,
    INTERVAL_BY,
    JOIN,
    JOIN_ON,
    ORDER_BY,
    PARTITION_BY,
    SELECT,
    WHERE,
    WINDOW_ON,
    WINDOW_UNTIL
};

inline std::string toString(SqlFragment type) {
    switch (type) {
        case SqlFragment::NONE:     return "NONE";
        case SqlFragment::FROM:  return "FROM";
        case SqlFragment::GROUP_BY:  return "GROUP_BY";
        case SqlFragment::HAVING: return "HAVING";
        case SqlFragment::INSERT: return "INSERT";
        case SqlFragment::INTERVAL_BY: return "INTERVAL_BY";
        case SqlFragment::JOIN: return "JOIN";
        case SqlFragment::JOIN_ON: return "JOIN_ON";
        case SqlFragment::ORDER_BY: return "ORDER_BY";
        case SqlFragment::PARTITION_BY: return "PARTITION_BY";
        case SqlFragment::SELECT: return "SELECT";
        case SqlFragment::WHERE: return "WHERE";
        case SqlFragment::WINDOW_ON: return "WINDOW_ON";
        case SqlFragment::WINDOW_UNTIL: return "WINDOW_UNTIL";
        default:                      return "UNKNOWN";
    }
}
#endif