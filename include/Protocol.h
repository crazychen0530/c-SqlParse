#ifndef SQL_PROTOCOL_H
#define SQL_PROTOCOL_H

#include <string>
#include <algorithm>
#include <memory>
#include "InvalidProtocolException.h"
#include "XStringUtils.h"
enum class Protocol {
    NONE,
    VSW,
    STF
};

class ProtocolUtils {
public:
    static Protocol get(const std::string& name) {
        if (!name.empty()) {
            std::string n = XStringUtils::toLowerCase(XStringUtils::trim(name));
            if (n.rfind("protocol.", 0) == 0) { // starts with "protocol."
                if (n == "protocol.vsw") {
                    return Protocol::VSW;
                } else if (n == "protocol.stf") {
                    return Protocol::STF;
                } else {
                    throw InvalidProtocolException("SQL_SYNTAX_INVALID_PROTOCOL: " + name);
                }
            }
        }
        return Protocol::NONE;
    }
};

#endif // SQL_PROTOCOL_H
