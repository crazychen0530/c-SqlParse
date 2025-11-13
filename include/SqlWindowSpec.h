#ifndef SQL_WINDOW_SPEC_H
#define SQL_WINDOW_SPEC_H
#include <string>

class SqlWindowSpec {
    protected:
        SqlWindowSpec(const std::string& type) : type(type){}
    private:
        std::string type;
        std::string kind;
        std::string keys;
        std::string sorts;
        std::string having;
    public:
        virtual void validate() const = 0;


        virtual ~SqlWindowSpec() = default;
        std::string getType(){
            return this->type;
        }

        std::string getKind(){
            return this->kind;
        }

        void setKind(const std::string& kind){
            this->kind = kind;
        }

        std::string getKeys(){
            return this->keys;
        }

        void setKeys(const std::string& keys){
            this->keys = keys;
        }

        std::string getSorts(){
            return this->sorts;
        }

        void setSorts(const std::string& sorts){
            this->sorts = sorts;
        }

        std::string getHaving(){
            return this->having;
        }

        void setHaving(const std::string& having){
            this->having = having;
        }
};  


#endif