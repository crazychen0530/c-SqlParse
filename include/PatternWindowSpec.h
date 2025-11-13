#ifndef PATTERN_WINDOW_SEPC_H
#define PATTERN_WINDOW_SEPC_H

#include "SqlWindowSpec.h"
#include <string>
class PatternWindowSpec : public SqlWindowSpec {
    private:
        std::string enter = "";   
        std::string exit = "";
    public:
        void validate() const override{
            // Add validation logic if needed
        }
        PatternWindowSpec() : SqlWindowSpec("pattern"){}

        std::string getEnter(){
            return this->enter;
        }
        
        void setEnter(const std::string enter){
            this->enter = enter;
        }

        std::string getExit(){
            return this->exit;
        }

        void setExit(const std::string exit){
            this->exit = exit;
        }
};

#endif