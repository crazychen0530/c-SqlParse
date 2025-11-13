#include "../include/SqlDistributedPlanner.h"
#include "../include/SqlRelation.h"
#include <algorithm> 
#include "../../exd-compute-lib-v2/source/exd/pervasive/iot/common/util/mutable/MutableInt.h"
#include "../../exd-compute-lib-v2/source/exd/pervasive/iot/common/util/ClassDefinition.h"
#include "../include/ProtocolRelation.h"
std::shared_ptr<SqlDistributedPlan> SqlDistributedPlanner::plan(const std::shared_ptr<SqlStatement>& stmt){
    std::vector<std::shared_ptr<ClassDefinition>> cloudSteps;
    std::vector<std::shared_ptr<ClassDefinition>> edgeSteps;
    std::shared_ptr<MutableInt> edgeRunnable = std::make_shared<MutableInt>(1);
    std::shared_ptr<MutableInt> currentId = std::make_shared<MutableInt>(-1);
    std::shared_ptr<MutableInt> lastSpool = std::make_shared<MutableInt>(-1);

    plan(stmt,cloudSteps,edgeSteps,edgeRunnable,currentId,lastSpool);

    std::vector<std::string> cloudPlan;
    for(auto step : cloudSteps){
        cloudPlan.push_back(step->toString());
    }

    std::vector<std::string> edgePlan;
    for(auto step : edgeSteps){
        edgePlan.push_back(step->toString());
    }
    return std::make_shared<SqlDistributedPlan>(cloudPlan,edgePlan);
}

 
void SqlDistributedPlanner::plan(const std::shared_ptr<SqlStatement>& stmt,
                std::vector<std::shared_ptr<ClassDefinition>>& cloudSteps,
                std::vector<std::shared_ptr<ClassDefinition>>& edgeSteps,
                std::shared_ptr<MutableInt> edgeRunnable,
                std::shared_ptr<MutableInt> currentId,
                std::shared_ptr<MutableInt> lastSpool){
                    if(auto q = std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())){
                        plan(q->getStatement(),cloudSteps,edgeSteps,edgeRunnable,currentId,lastSpool);
                        lastSpool->set(currentId->getValue());
                    }else{
                        currentId->increment();

                        std::shared_ptr<TableRelation> table = std::dynamic_pointer_cast<TableRelation>(stmt->getFrom());
                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->setClassName("Input");
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                        step->addParameter("name",table->getName());

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,true);

                        lastSpool->set(currentId->getValue());
                    }

                    if(XStringUtils::isNotBlank(stmt->getWhere())){
                        currentId->increment();

                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->setClassName("Filter");
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("input","s_" + std::to_string(lastSpool->getValue()));
                        step->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                        step->addParameter("condition",stmt->getWhere());

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,true);

                        lastSpool->set(currentId->getValue());
                    }

                    if(stmt->getJoin() != nullptr){
                        std::shared_ptr<SqlJoinSpec> join =stmt->getJoin();
                        std::shared_ptr<MutableInt> fromSpool = std::make_shared<MutableInt>(lastSpool->getValue());
                        if(auto q = std::dynamic_pointer_cast<QueryRelation>(join->getRelation())){
                            plan(q->getStatement(),cloudSteps,edgeSteps,edgeRunnable,currentId,lastSpool);
                        }else{
                            currentId->increment();

                            std::shared_ptr<TableRelation> table = std::dynamic_pointer_cast<TableRelation>(join->getRelation());
                            std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                            step->setClassName("Input");
                            step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                            step->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                            step->addParameter("name",table->getName());

                            addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,true);

                            lastSpool->set(currentId->getValue());
                        }

                        currentId->increment();

                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("input","s_" + std::to_string(fromSpool->getValue()));
                        step->addAttribute("input2","s_" + std::to_string(lastSpool->getValue()));
                        step->addAttribute("output","s_" + std::to_string(currentId->getValue()));

                        const std::string leftAlias = stmt->getFrom()->getAlias(),rightAlias = join->getRelation()->getAlias();
                        std::vector<std::string> leftTerms ,rightTerms;
                        const std::string joinType = XStringUtils::toLowerCase(toString(join->getType()));
                        if(ExpressionModelUtils::isReduceJoinCondition(join->getConditionExp(),leftAlias,rightAlias,leftTerms,rightTerms)){
                            step->setClassName("ReduceJoin");
                            step->addParameter("selects",stmt->getSelects());
                            step->addParameter("lefts",ExpressionModelUtils::mergeTerms(leftTerms));
                            step->addParameter("rights",ExpressionModelUtils::mergeTerms(rightTerms));
                            step->addParameter("left_alias",leftAlias);
                            step->addParameter("right_alias",rightAlias);
                            step->addParameter("join_type",joinType);
                        }else{
                            step->setClassName("NestedJoin");
                            step->addParameter("selects",stmt->getSelects());
                            step->addParameter("condition",join->getCondition());
                            step->addParameter("left_alias",leftAlias);
                            step->addParameter("right_alias",rightAlias);
                            step->addParameter("join_type",joinType);
                        }

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,false);

                        lastSpool->set(currentId->getValue());
                    }else if(XStringUtils::isNotBlank(stmt->getGroupbys()) || (stmt->getInterval() != nullptr && stmt->getInterval()->getTimeAmount() > 0)){
                        currentId->increment();

                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("input","s_" + std::to_string(lastSpool->getValue()));
                        step->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                        if(XStringUtils::isNotBlank(stmt->getGroupbys())){
                            step->setClassName("GroupBy");
                            step->addParameter("keys",stmt->getGroupbys());
                            step->addParameter("selects",stmt->getSelects());
                        }else{
                            step->setClassName("StreamAggregate");
                            step->addParameter("selects",stmt->getSelects());
                            step->addParameter("time",stmt->getInterval()->getInterval());
                            step->addParameter("interval",std::to_string(stmt->getInterval()->getTimeAmount()));
                            step->addParameter("time_unit",stmt->getInterval()->getTimeUnit());
                        }

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,false);

                        lastSpool->set(currentId->getValue());

                        if(XStringUtils::isNotBlank(stmt->getHaving())){
                            currentId->increment();

                            std::shared_ptr<ClassDefinition> hvstep = std::make_shared<ClassDefinition>();
                            hvstep->setClassName("Filter");
                            hvstep->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                            hvstep->addAttribute("input","s_" + std::to_string(lastSpool->getValue()));
                            hvstep->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                            hvstep->addParameter("condition",stmt->getHaving());

                            addStep(stmt,hvstep,cloudSteps,edgeSteps,edgeRunnable,false);

                            lastSpool->set(currentId->getValue());
                        }
                    }else if(stmt->getWindow() != nullptr){
                        currentId->increment();

                        std::shared_ptr<SqlWindowSpec> spec = stmt->getWindow();
                        const std::string windowKind = "window" == XStringUtils::toLowerCase(spec->getKind()) ? "Window" : "Session";
                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("input","s_" + std::to_string(lastSpool->getValue()));
                        step->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                        step->addParameter("keys",spec->getKeys());
                        step->addParameter("sorts",spec->getSorts());

                        if("pattern" == XStringUtils::toLowerCase(spec->getType())){
                            std::shared_ptr<PatternWindowSpec> pwspec = std::dynamic_pointer_cast<PatternWindowSpec>(spec);
                            step->setClassName("Pattern"+ windowKind);
                            step->addParameter("enter",pwspec->getEnter());
                            step->addParameter("exit",pwspec->getExit());
                            step->addParameter("selects",stmt->getSelects());
                            step->addParameter("validity",pwspec->getHaving());
                        }else if("sliding" == XStringUtils::toLowerCase(spec->getType())){
                            std::shared_ptr<SlidingWindowSpec> swspec = std::dynamic_pointer_cast<SlidingWindowSpec>(spec);
                            step->setClassName("Sliding"+ windowKind);
                            step->addParameter("inclusion",swspec->getInclusion());
                            step->addParameter("selects",stmt->getSelects());
                            step->addParameter("validity",swspec->getHaving());
                        }else{
                            std::shared_ptr<TumblingWindowSpec> twspec = std::dynamic_pointer_cast<TumblingWindowSpec>(spec);
                            step->setClassName("Tumbling"+ windowKind);
                            step->addParameter("inclusion",twspec->getInclusion());
                            step->addParameter("selects",stmt->getSelects());
                            step->addParameter("validity",twspec->getHaving());
                        }

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,false);

                        lastSpool->set(currentId->getValue());
                    }else if(!stmt->isSelectAll()){
                        currentId->increment();

                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->setClassName("Project");
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("input","s_" + std::to_string(lastSpool->getValue()));
                        step->addAttribute("output","s_" + std::to_string(currentId->getValue()));
                        step->addParameter("selects",stmt->getSelects());

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,false);

                        lastSpool->set(currentId->getValue());

                    }
                    
                    if (stmt->getLimit() > 0) {
                        std::vector<std::shared_ptr<ClassDefinition>>& steps =
                            stmt->isEdgeRunnable() ? edgeSteps : cloudSteps;

                        if (steps.size() == 1) {
                            steps.at(0)->addParameter("max_num_readers", "1");
                            steps.at(0)->addParameter("limit_count", std::to_string(stmt->getLimit()));
                        } else if (
                            steps.size() == 2 &&
                            selectOnly(stmt) &&
                            (
                                std::dynamic_pointer_cast<TableRelation>(stmt->getFrom()) ||
                                (
                                    std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom()) &&
                                    selectOnly(std::dynamic_pointer_cast<QueryRelation>(stmt->getFrom())->getStatement())
                                )
                            ) &&
                            (stmt->getInto() == nullptr || std::dynamic_pointer_cast<TableRelation>(stmt->getInto()))
                        ) {
                            steps.at(0)->addParameter("max_num_readers", "1");
                            steps.at(0)->addParameter("limit_count", std::to_string(stmt->getLimit()));
                        } else {
                            currentId->increment();

                            std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                            step->setClassName("Take");
                            step->addAttribute("id", "d_" + std::to_string(currentId->getValue()));
                            step->addAttribute("input", "s_" + std::to_string(lastSpool->getValue()));
                            step->addAttribute("output", "s_" + std::to_string(currentId->getValue()));
                            step->addParameter("rows", std::to_string(stmt->getLimit()));

                            steps.push_back(step);
                            lastSpool->set(currentId->getValue());
                        }
                    }


                    if(stmt->getInto() != nullptr){
                        currentId->increment();

                        if(std::dynamic_pointer_cast<ProtocolRelation>(stmt->getInto())){
                            throw std::runtime_error("protocol into not supported in distributed query");
                        }
                        std::shared_ptr<TableRelation> table = std::dynamic_pointer_cast<TableRelation>(stmt->getInto());
                        std::shared_ptr<ClassDefinition> step = std::make_shared<ClassDefinition>();
                        step->setClassName("Output");
                        step->addAttribute("id","d_" + std::to_string(currentId->getValue()));
                        step->addAttribute("input","s_" + std::to_string(lastSpool->getValue()));
                        step->addParameter("name",table->getName());

                        addStep(stmt,step,cloudSteps,edgeSteps,edgeRunnable,false);
                    }
                }

   
void SqlDistributedPlanner::addStep(const std::shared_ptr<SqlStatement>& stmt,
                    const std::shared_ptr<ClassDefinition>& step,
                    std::vector<std::shared_ptr<ClassDefinition>>& cloudSteps,
                    std::vector<std::shared_ptr<ClassDefinition>>& edgeSteps,
                    std::shared_ptr<MutableInt> edgeRunnable,
                    bool switchOverPushdown){
                        if(edgeRunnable->getValue() == 0){
                            cloudSteps.push_back(step);
                            return;
                        }

                        if(switchOverPushdown || stmt->isEdgeRunnable()){
                            edgeSteps.push_back(step);
                            return;
                        }

                        edgeRunnable->set(0);
                        cloudSteps.push_back(step);
                    }

bool SqlDistributedPlanner::selectOnly(const std::shared_ptr<SqlStatement>& stmt){
    return XStringUtils::isBlank(stmt->getWhere()) && XStringUtils::isBlank(stmt->getGroupbys()) && stmt->getWindow() == nullptr;
}