#ifndef PROTOCOL_RELATION_H
#define PROTOCOL_RELATION_H

#include <memory>
#include "SqlRelation.h"
#include "Protocol.h"


class ProtocolRelation : public SqlRelation {
private:
    Protocol protocol;

public:
    explicit ProtocolRelation(const Protocol& protocol = Protocol::NONE)
        : protocol(protocol) {}

    /**
     * 获取协议类型。
     */
    Protocol getProtocol() const {
        return protocol;
    }

    /**
     * 设置协议类型。
     */
    void setStatement(const Protocol& protocol) {
        this->protocol = (protocol == Protocol::NONE ? Protocol::NONE : protocol);
    }

    /**
     * 获取别名。
     * 该类型 relation 没有别名，返回空字符串。
     */
    std::string getAlias() const override {
        return "";
    }

    void setAlias(const std::string& /*alias*/) override {

    }
};

#endif // PROTOCOL_RELATION_H
