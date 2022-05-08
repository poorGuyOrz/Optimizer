#pragma once
#include <stack>

#include "rules.h"
// class OptimizerTask;       // Abstract class for all task classes
// class OptimizerTaskStack;  // Pending tasks - some structure which contains all tasks waiting to execute
// class OptimizeGroupTask;   // Optimize a Group - find the cheapest plan in the group satisfying a context
// class OptimizeExprTask;    // Optimize an Expression - Fire all relevant rules for this expression
// class ExploreGroupTask;    // Explore a group - Fire all transformation rules in this group.
// class OptimizeInputTask;            // Optimize inputs - determine if this expression satisfies the current context
// class ApplyRuleTask;       // Apply a single rule to a single MExression

// Pair of rule and promise, used to sort rules according to their promise
typedef struct MOVE {
  int promise;
  RULE *rule;
} MOVE;

// Pair of expr and cost value, used to sort expr according to their cost
typedef struct AFTERS {
  MExression *m_expr;
  Cost *cost;
} AFTERS;

/*
        ============================================================
        OptimizerTask
        ============================================================
        A task is an activity within the search process.  The original task
        is to optimize the entire query.  Tasks create and schedule each
        other; when no pending tasks remain, optimization terminates.

        In Cascades and Columbia, tasks store winners in memos; they do not
        actually produce a best plan.  After the optimization terminates,
        SearchSpace::CopyOut() is called to print the best plan.

        OptimizerTask is an abstract class.  Its subclasses are specific tasks.


        Tasks must destroy themselves when done!
*/

class OptimizerTask {
 protected:
  int ContextID;     // Index to CONT::vc, the shared set of contexts
  int ParentTaskNo;  // The task which created me

 public:
  OptimizerTask(int ContextID, int ParentTaskNo) : ContextID(ContextID), ParentTaskNo(ParentTaskNo){};
  ~OptimizerTask(){};
  virtual string Dump() = 0;
  virtual void perform() = 0;
};

/*
  ============================================================
  OptimizerTaskStack - Pending Tasks
  ============================================================
  This collection of undone tasks is currently stored as a stack.
  Other structures are certainly appropriate, but in any case dependencies
  must be stored.  For example, a directed graph could be used to
  parallelize optimization.
*/

class OptimizerTaskStack {
 private:
  vector<OptimizerTask *> task_stack_;

 public:
  ~OptimizerTaskStack() {
    while (!empty()) delete pop();
  };

  bool empty() { return task_stack_.empty(); };
  void push(OptimizerTask *task) {
    task_stack_.push_back(task);
    if (COVETrace) OutputCOVE << "PushTaskList {" << task->Dump() << "}" << endl;
  };
  OptimizerTask *pop() {
    auto task = task_stack_.back();
    task_stack_.pop_back();
    if (COVETrace) OutputCOVE << "PopTaskList  {" << task->Dump() << "}" << endl;
    return task;
  };

  string Dump() {
    string os;
    if (empty())
      os = "Task Stack is empty!!";
    else {
      int count1 = task_stack_.size() - 5;
      int count = 0;
      for (auto &&task : task_stack_) {
        if (count >= count1) os += "    task: " + to_string(count) + "   " + task->Dump() + "\n";
        count++;
      }
    }
    return os;
  };
};

/*
     ============================================================
     OptimizeGroupTask - Task to Optimize a Group
     ============================================================
     This task finds the cheapest multiplan in this group, for a given
     context, and stores it (with the context) in the group's winner's circle.
     If there is no cheapest plan (e.g. the upper bound cannot be met),
     the context is stored in the winner's circle with a null plan.

     This task generates all relevant[相關的] expressions in the group, costs them
     and chooses the cheapest one.

     The determination[計算] of what is "cheapest" may include such issues as
     robustness, e.g. this task may choose the multiplan with smallest
     cost+variance.  Here variance is some measure of how much the cost
     varies as the statistics vary.

     There are at least two ways to implement this task; we will discover which
     is best.
/// ????????????
     First, Goetz' original way, is to process each multiexpression separately,
     in the order they appear in the list/collection of multiexpressions.  To
     process an expression means to determine all relevant rules, then fire them
     in order of promise().

     Second, hinted at by Goetz, applies only when there are multiple expressions
     in the group, e.g. after exploring.  Here we can consider all rules on all
     expressions, and fire them in order of their promise.  Promise here may include
     a lower bound estimate for the expressions.

*/

class OptimizeGroupTask : public OptimizerTask {
 private:
  int GrpID;  // Which group to optimize
  bool Last;  // if this task is the last task for this group
 public:
  OptimizeGroupTask(int GrpID, int ContextID, int parent_task_no, bool last = true)
      : OptimizerTask(ContextID, parent_task_no), GrpID(GrpID), Last(last) {
    // if INFBOUND flag is on, set the bound to be INF
#ifdef INFBOUND
    Cost *INFCost = new Cost(-1);
    CONT::vc[ContextID]->SetUpperBound(*INFCost);
#endif
  };
  ~OptimizeGroupTask(){};

  // Optimize the group by searching for a winner for the context.
  // Initialize or update the winner for the context's property
  void perform();

  string Dump();

};  // OptimizeGroupTask

/*

     ============================================================
     ExploreGroupTask - Task to Explore the Group
     ============================================================
     Some rules require that their inputs contain particular (target) operators.  For
     example, the associativity rule requires that one input contain a join.  The
     ExploreGroupTask task explores a group by creating all target operators that could
     belong to the group, e.g., fire whatever rules are necessary to create all
     joins that could belong to the group.

     The simplest implementation of this task is to generate all logical
     multiexpressions in the group.

     More sophisticated implementations would fire only those rules which might
     generate the target operator.  But it is hard to tell what those rules
     are (note that it may require a sequence of rules to get to the target).
     Furthermore, on a second ExploreGroupTask task for a second target it may be
     difficult to use the results of the first visit intelligently.

     Because we are using the simple implementation, we do not need an E_EXPR task.
     Instead we will use the OptimizeGroupTask task but ensure that it fires only transformation rules.

     If we are lucky, groups will never need to be explored: physical rules are fired first,
     and the firing of a physical rule will cause all inputs to be optimized, therefore explored.
     This may not work if we are using pruning: we might skip physical rule firings because of
     pruning, then need to explore.  For now we will put a flag in ExploreGroupTask to catch when it does not work.
 */
class ExploreGroupTask : public OptimizerTask {
 private:
  int GrpID;  // Group to be explored
  bool Last;  // is it the last task in this group
 public:
  ExploreGroupTask(int GrpID, int ContextID, int parent_task_no, bool last = false);
  ~ExploreGroupTask(){};

  void perform();

  string Dump();
};

/*
   ============================================================
   OptimizeExprTask - Task to Optimize a multi-expression
   ============================================================
   This task is needed only if we implement OptimizeGroupTask in the original way.
   This task fires all rules for the expression, in order of promise.
   when it is used for exploring, fire only transformation rules to prepare for a transform
*/

class OptimizeExprTask : public OptimizerTask {
 private:
  MExression *MExpr;   // Which expression to optimize
  const bool explore;  // if this task is for exploring  Should not happen - see ExploreGroupTask
  bool Last;           // if this task is the last task for the group

 public:
  OptimizeExprTask(MExression *mexpr, bool explore, int ContextID, int parent_task_no, bool last = false);

  ~OptimizeExprTask() {
    if (Last) {
      Group *group = Ssp->GetGroup(MExpr->GetGrpID());
      if (!explore) {
        CONT *LocalCont = CONT::vc[ContextID];
        // What prop is required of
        PHYS_PROP *LocalReqdProp = LocalCont->GetPhysProp();
        WINNER *Winner = group->GetWinner(LocalReqdProp);

        // mark the winner as done
        if (Winner) assert(!Winner->GetDone());

        Winner->SetDone(true);
        // this's still the last applied rule in the group,
        // so mark the group with completed optimizing
        group->set_optimized(true);
      } else
        group->set_explored(true);
    }
  };

  string Dump();

  void perform();

};  // OptimizeExprTask

/*

      ========================================================
     OptimizeInputTask - Task to Optimize inputs
     ========================================================
     This task is rather misnamed.  It:
     1) Determines whether the (physical) MExpr satisfies the task's context
     2) As part of (1), it may optimize/cost some inputs
     3) It may adjust the current context and the current winner.
     It may use bounds, primarily upper bounds, in its work.

     Member data InputNo, initially 0, indicates which input has been
     costed.  This task is unique in that it does not terminate after
     scheduling other tasks.  If the current input needs to be optimized,
     it first pushes itself onto the stack, then it schedules
     the optimization of the current input.  If and when inputs are all
     costed, it calculates the cost of the entire physical expression.
 */

class OptimizeInputTask : public OptimizerTask {
 private:
  MExression *MExpr;  // expression whose inputs we are optimizing
  int arity;
  int InputNo;      // input currently being or about to be optimized, initially 0
  int PrevInputNo;  // keep track of the previous optimized input no
  Cost *LocalCost;  // the local cost of the mexpr
  bool Last;        // if this task is the last task for the group
  int ContNo;       // keep track of number of contexts

  // Costs and properties of input winners and groups.  Computed incrementally
  //  by this method.
  Cost **InputCost;
  LOG_PROP **InputLogProp;

 public:
  OptimizeInputTask(MExression *MExpr, int ContextID, int ParentTaskNo, bool last = false, int ContNo = 0);

  ~OptimizeInputTask();

  // return the new upper bd for the input
  Cost *NewUpperBd(Cost *OldUpperBd, int input);
  void perform();

  string Dump();

};  // OptimizeInputTask

/*
   ============================================================
   ApplyRuleTask - Task to Apply a Rule to a Multi-Expression
   ============================================================
*/

class ApplyRuleTask : public OptimizerTask {
 private:
  RULE *Rule;          // rule to apply
  MExression *MExpr;   // root of expr. before rule
  const bool explore;  // if this task is for exploring
  bool Last;           // if this task is the last task for the group
 public:
  ApplyRuleTask(RULE *rule, MExression *mexpr, bool explore, int ContextID, int parent_task_no, bool last = false);

  ~ApplyRuleTask();

  void perform();

  string Dump();

};  // ApplyRuleTask
