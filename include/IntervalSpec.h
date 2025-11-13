#ifndef INTERVAL_SEPC_H
#define INTERVAL_SEPC_H
#include <string>
#include <memory>
#include "ExpressionContainer.h"
#include "StringData.h"
#include "EngineException.h"
#include "XStringUtils.h"
class IntervalSpec {
    private:
        std::shared_ptr<ExpressionContainer> interval = std::make_shared<ExpressionContainer>();
        int timeAmount = 0;
        std::string timeUnit = "second";
    public:
        std::string getInterval(){
            return this->interval->getCacheData()->toString();
        }

        void setInterval(const std::string& time){
            try{
                this->interval->require(std::make_shared<StringData>(time),"empty interval expression");
            }catch(const EngineException& e){
                throw EngineException(std::string("SQL_SYNTAX_INTERVAL_")+e.what());
            }
        }

        int getTimeAmount() const {
            return this->timeAmount;
        }

        void setTimeAmount(int amount){
            this->timeAmount = amount >= 0 ? amount : 0;
        }

        std::string getTimeUnit() const {
            return this->timeUnit;
        }

        void setTimeUnit(const std::string unit){
            if(XStringUtils::isBlank(unit)){
                this->timeUnit = "second";
            }

            const std::string s = XStringUtils::toLowerCase(XStringUtils::trim(unit));
            if(s == "millisecond" || s == "second" || s == "minute" || s == "hour" ){
                this->timeUnit = s;
            }else{
                this->timeUnit = "second";
            }
        }
};

#endif