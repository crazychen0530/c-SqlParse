#ifndef SLIDING_WINDOW_SPEC_H
#define SLIDING_WINDOW_SPEC_H

#include "SqlWindowSpec.h"

class SlidingWindowSpec : public SqlWindowSpec{
    private:
        std::string inclusion = "";
    public:
        void validate() const override{
            // Add validation logic if needed
        }
        SlidingWindowSpec() : SqlWindowSpec("sliding"){}
        
        std::string getInclusion(){
            return this->inclusion;
        }
        void setInclusion(const std::string inclusion){
            this->inclusion = inclusion;
        }

};

#endif