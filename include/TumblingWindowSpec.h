#ifndef TUMBLING_WINDOW_SPEC_H
#define TUMBLING_WINDOW_SPEC_H

#include "SqlWindowSpec.h"
#include <string>  

class TumblingWindowSpec : public SqlWindowSpec{
    private:
        std::string inclusion = "";

    public:
        void validate() const override{
            // Add validation logic if needed
        }
        TumblingWindowSpec() : SqlWindowSpec("tumbling"){}
        

        std::string getInclusion(){
            return this->inclusion;
        }

        void setInclusion(const std::string inclusion){
            this->inclusion = inclusion;
        }
};

#endif