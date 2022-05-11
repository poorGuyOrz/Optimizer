#include "../header/tasks.h"

#include "../header/physop.h"
#include "../header/stdafx.h"

/* Function to compare the promise of rule applications */
int compare_moves(void const *x, void const *y) {
  // Cast arguments back to pointers to MOVEs
  MOVE *a = (MOVE *)x;
  MOVE *b = (MOVE *)y;

  int result = 0;
  if (a->promise > b->promise)
    result = -1;
  else if (a->promise < b->promise)
    result = 1;
  else
    result = 0;

  return result;
}

/* Function to compare the cost of mexprs */
int compare_afters(void const *x, void const *y) {
  // Cast arguments back to pointers to AFTER
  AFTERS *a = (AFTERS *)x;
  AFTERS *b = (AFTERS *)y;

  int result = 0;
  if ((*a->cost) < (*b->cost))
    result = -1;
  else if ((*a->cost) > (*b->cost))
    result = 1;
  else
    result = 0;

  return result;
}

/*
OptimizeGroupTask::perform
{
    see search_circle in declaration of class Group, for notation

    Call search_circle
    If cases (1) or (2),
        terminate this task. //circle is prepared
    Cases (3) and (4) remain.  More search.

            IF (Group is not optimized)
            assert (this is case 3, never searched for this property before)
            if (property is ANY)
                    Push OptimizeExprTask on first logical expression
                            add a winner for this context to the circle, with null plan
                              (i.e., initialize the winner's circle for this property.)
            else
                    Push OptimizeGroupTask on this group with current context
                    Push OptimizeGroupTask on this group, with new context: ANY property and
                            cost = current context cost - appropriate enforcer cost, last task.
                                    This is valid since the result of the ANY search will only be used with
                                    the enforcer.  If the cost is negative, then the enforcer cannot be used,
                                    so the ANY winner should be null.
                                    Since this is somewhat complex and prone to error, we will omit it for now
                                    and use the original context's cost
            else (Group is optimized)
                    if (property is ANY)
                    assert (this is case 4)
                    push OptimizeInputTask on all physical mexprs
            else (property is not ANY)
                    Push OptimizeInputTask on all physical mexprs with current context, last one is last task
                    If case (3) [i.e. appropriate enforcer is not in group], Push ApplyRuleTask on
                     enforcer rule, not the last task
                            add a winner for this context to the circle, with null plan.
                              (i.e., initialize the winner's circle for this property.)

*/
/*

  group中的所有表達式，
    對於邏輯表達式。使用OptimizeExprTask去優化表達式
    對於物理表達式。使用optinputtask優化
*/
void OptimizeGroupTask::perform() {
  auto GrpID = group_->GetGroupID();
  PTRACE("OptimizeGroupTask: " << GrpID << " is performing "
                               << "Last flag is " << Last);

  PTRACE("Context ID: " << ContextID << " , " << CONT::vc[ContextID]->Dump());

  MExression *FirstLogMExpr = group_->GetFirstLogMExpr();

  if (FirstLogMExpr->GetOp()->is_const()) {
    PTRACE("Group " << GrpID << " is const group");
    MExression *WPlan = new MExression(*FirstLogMExpr);
    group_->NewWinner(new PHYS_PROP(any), WPlan, new Cost(0), true);
    return;
  }

  bool moreSearch, SCReturn;

  CONT *LocalCont = CONT::vc[ContextID];
  PHYS_PROP *LocalReqdProp = LocalCont->GetPhysProp();  // What prop is required
  Cost *LocalCost = LocalCont->GetUpperBd();

  SCReturn = group_->search_circle(LocalCont, moreSearch);

  // If case (2) or (1), terminate this task
  if (!moreSearch) {
    PTRACE("Winner's circle is prepared so terminate this task");
    delete this;
    return;
  }

  PTRACE("Group is " << (group_->is_optimized() ? "" : "not") << " optimized");
  if (!group_->is_optimized()) {
    assert(moreSearch && !SCReturn);  // assert (this is case 3)
    // if (property is ANY)
    if (LocalReqdProp->GetOrder() == any) {
      PTRACE("add winner with null plan, push OptimizeExprTask on 1st logical expression");
      group_->NewWinner(LocalReqdProp, nullptr, new Cost(*LocalCost), false);
      PTasks.push(new OptimizeExprTask(FirstLogMExpr, false, ContextID, TaskNo, true));
    } else {
      PTRACE("Push OptimizeGroupTask with current context, another with ANY context");
      assert(LocalReqdProp->GetOrder() == sorted);  // temporary
      PTasks.push(new OptimizeGroupTask(group_, ContextID, TaskNo, true));
      Cost *NewCost = new Cost(*(LocalCont->GetUpperBd()));
      CONT *NewContext = new CONT(new PHYS_PROP(any), NewCost, false);
      CONT::vc.push_back(NewContext);
      PTasks.push(new OptimizeGroupTask(group_, CONT::vc.size() - 1, TaskNo, true));
    }
  } else  // Group is optimized
  {
    // if (property is ANY)
    // assert (this is case 4)
    // push OptimizeInputTask on all physical mexprs
    vector<MExression *> PhysMExprs;
    int count = 0;
    if (LocalReqdProp->GetOrder() == any) {
      PTRACE("push OptimizeInputTask on all physical mexprs");
      assert(moreSearch && SCReturn);
      for (MExression *PhysMExpr = group_->GetFirstPhysMExpr(); PhysMExpr; PhysMExpr = PhysMExpr->GetNextMExpr()) {
        PhysMExprs.push_back(PhysMExpr);
        count++;
      }
      // push the last PhysMExpr
      if (--count >= 0) {
        PTRACE("pushing OptimizeInputTask " << PhysMExprs[count]->Dump());
        PTasks.push(new OptimizeInputTask(PhysMExprs[count], ContextID, TaskNo, true));
      }
      // push other PhysMExpr
      while (--count >= 0) {
        PTRACE("pushing OptimizeInputTask " << PhysMExprs[count]->Dump());
        PTasks.push(new OptimizeInputTask(PhysMExprs[count], ContextID, TaskNo, false));
      }
    } else  // property is not ANY)
    {
      assert(LocalReqdProp->GetOrder() == sorted);  // temporary
      // Push OptimizeInputTask on all physical mexprs with current context, last one is last task
      PTRACE("Push OptimizeInputTask on all physical mexprs");
      for (MExression *PhysMExpr = group_->GetFirstPhysMExpr(); PhysMExpr; PhysMExpr = PhysMExpr->GetNextMExpr()) {
        PhysMExprs.push_back(PhysMExpr);
        count++;
      }
      // push the last PhysMExpr
      if (--count >= 0) {
        PTRACE("pushing OptimizeInputTask " << PhysMExprs[count]->Dump());
        PTasks.push(new OptimizeInputTask(PhysMExprs[count], ContextID, TaskNo, true));
      }
      // push other PhysMExpr
      while (--count >= 0) {
        PTRACE("pushing OptimizeInputTask " << PhysMExprs[count]->Dump());
        PTasks.push(new OptimizeInputTask(PhysMExprs[count], ContextID, TaskNo, false));
      }

      // If case (3) [i.e. appropriate enforcer is not in group], Push ApplyRuleTask on
      //	enforcer rule, not the last task
      if (!SCReturn) {
        PTRACE("Push ApplyRuleTask on enforcer rule");
        if (LocalReqdProp->GetOrder() == sorted) {
          Rule *Rule = (*ruleSet)[R_SORT_RULE];
          PTasks.push(new ApplyRuleTask(Rule, FirstLogMExpr, false, ContextID, TaskNo, false));
        } else {
          assert(false);
        }
      }
      // add a winner to the circle, with null plan.
      //(i.e., initialize the winner's circle for this property.)
      PTRACE("Init winner's circle for this property");
      if (moreSearch && !SCReturn) group_->NewWinner(LocalReqdProp, nullptr, new Cost(*LocalCost), false);
    }
  }
  delete this;

}  // OptimizeGroupTask::perform

string OptimizeGroupTask::Dump() {
  string os;
  os = "OptimizeGroupTask group: " + to_string(group_->GetGroupID()) + ", parent task: " + to_string(ParentTaskNo) +
       ", Last: " + to_string(Last);
  os += ", " + CONT::vc[ContextID]->Dump();
  return os;
}

void ExploreGroupTask::perform() {
  auto GrpID = group_->GetGroupID();
  PTRACE("ExploreGroupTask " << GrpID << " performing");
  PTRACE("Context ID: " << ContextID << " , " << CONT::vc[ContextID]->Dump());

  if (group_->is_optimized() || group_->is_explored()) {
    delete this;
    return;
  }

  if (group_->is_exploring())
    assert(false);
  else {
    // the group will be explored, let other tasks don't do it again
    group_->set_exploring(true);

    // mark the group not explored since we will begin exploration
    group_->set_explored(false);

    MExression *LogMExpr = group_->GetFirstLogMExpr();

    // only need to E_EXPR the first log expr,
    // because it will generate all logical exprs by applying appropriate rules
    // it won't generate dups because rule bit vector
    PTRACE("pushing OptimizeExprTask exploring " << LogMExpr->Dump());
    // this logical mexpr will be the last optimized one, mark it as the last task for this group
    PTasks.push(new OptimizeExprTask(LogMExpr, true, ContextID, TaskNo, true));
  }

  delete this;
}

string ExploreGroupTask::Dump() {
  string os;
  os = "ExploreGroupTask  group: " + to_string(group_->GetGroupID()) + ", parent task: " + to_string(ParentTaskNo) +
       ", Last: " + to_string(Last);
  os += ", " + CONT::vc[ContextID]->Dump();
  return os;
}

// ************  OptimizeExprTask ******************

OptimizeExprTask::OptimizeExprTask(MExression *mexpr, bool explore, int ContextID, int parent_task_no, bool last)
    : OptimizerTask(ContextID, parent_task_no),
      MExpr(mexpr),
      explore(explore),
      Last(last){};  // OptimizeExprTask::OptimizeExprTask

void OptimizeExprTask::perform() {
  PTRACE("OptimizeExprTask performing, " << (explore ? "exploring" : "optimizing") << " mexpr: " << MExpr->Dump());

  PTRACE("Context ID: " << ContextID << " , " << CONT::vc[ContextID]->Dump());
  PTRACE("Last flag is " << Last);

  if (explore) assert(MExpr->GetOp()->is_logical());  // explore is only for logical expression

  if (MExpr->GetOp()->is_item()) {
    PTRACE("expression is an item_op");
    // push the O_INPUT for this item_expr
    PTRACE("pushing OptimizeInputTask " << MExpr->Dump());
    PTasks.push(new OptimizeInputTask(MExpr, ContextID, TaskNo, true));
    delete this;
    return;
  }

  // identify valid and promising rules
  MOVE *Move = new MOVE[ruleSet->RuleCount];  // to collect valid, promising moves
  int moves = 0;                              // # of moves already collected
  for (int RuleNo = 0; RuleNo < ruleSet->RuleCount; RuleNo++) {
    Rule *Rule = (*ruleSet)[RuleNo];

    if (Rule == nullptr) continue;  // some rules may be turned off

    if (explore && Rule->GetSubstitute()->GetOp()->is_physical()) {
      PTRACE("Rejected rule  " << Rule->get_index());
      continue;  // only fire transformation rule when exploring
    }

    int Promise = Rule->promise(MExpr->GetOp(), ContextID);
    // insert a valid and promising move into the array
    if (Rule->top_match(MExpr->GetOp()) && Promise > 0) {
      Move[moves].promise = Promise;
      Move[moves++].rule = Rule;
      TopMatch[RuleNo]++;
    }
  }

  PTRACE(moves << " promising moves");

  // order the valid and promising moves by their promise
  qsort((char *)Move, moves, sizeof(MOVE), compare_moves);
  // optimize the rest rules in order of promise
  while (--moves >= 0) {
    bool Flag = false;
    if (Last)
    // this's the last task in the group,pass it to the new task
    {
      Last = false;  // turn off this, since it's no longer the last task
      Flag = true;
    }

    // push future tasks in reverse order (due to LIFO stack)
    Rule *Rule = Move[moves].rule;
    PTRACE("pushing rule " << Rule->GetName());

    // apply the rule
    PTasks.push(new ApplyRuleTask(Rule, MExpr, explore, ContextID, TaskNo, Flag));

    // for enforcer and expansion rules, don't explore patterns
    Expression *original = Rule->GetOriginal();
    if (original->GetOp()->is_leaf()) continue;

    // earlier tasks: explore all inputs to match the original pattern
    for (int input_no = original->GetArity(); --input_no >= 0;) {
      // only explore the input with arity > 0
      if (original->GetInput(input_no)->GetArity()) {
        // If not yet explored, schedule a task with new context
        int grp_no = (MExpr->GetInput(input_no));
        if (!Ssp->GetGroup(grp_no)->is_exploring()) {
          // ExploreGroupTask can not be the last task for the group
          PTasks.push(new ExploreGroupTask(Ssp->GetGroup(grp_no), ContextID, TaskNo, false));
        }
      }
    }  // earlier tasks: explore all inputs to match the original pattern

  }  // optimize in order of promise

  delete[] Move;
  delete this;
}  // OptimizeExprTask::perform

string OptimizeExprTask::Dump() {
  string os;
  os = "OptimizeExprTask  group: " + MExpr->Dump() + ", parent task: " + to_string(ParentTaskNo) +
       ", Last: " + to_string(Last) + ", explore: " + to_string(explore);
  os += ", " + CONT::vc[ContextID]->Dump();
  return os;
}

OptimizeInputTask::OptimizeInputTask(MExression *MExpr, int ContextID, int ParentTaskNo, bool last, int ContNo)
    : MExpr(MExpr), OptimizerTask(ContextID, ParentTaskNo), InputNo(-1), Last(last), PrevInputNo(-1), ContNo(ContNo) {
  assert(MExpr->GetOp()->is_physical() || MExpr->GetOp()->is_item());
  // We can only calculate cost for physical operators

  // Cache local properties
  Operator *Op = (PhysicalOperator *)(MExpr->GetOp());  // the op of the expr
  arity = Op->GetArity();                               // cache arity of mexpr

  // create the arrays of input costs and logical properties
  if (arity) {
    InputCost = new Cost *[arity];
    InputLogProp = new LOG_PROP *[arity];
  }
};

OptimizeInputTask::~OptimizeInputTask() {
  // localcost was new by find_local_cost, so need to delete it
  delete LocalCost;

  if (arity) {
    delete[] InputCost;
    delete[] InputLogProp;
  }

}  // OptimizeInputTask::~OptimizeInputTask

/*
OptimizeInputTask::perform

NOTATION
InputCost[]: Contains actual (or lower bound) costs of optimal inputs to G.
CostSoFar: LocalCost + sum of all InputCost entries.
G: the group being optimized.
IG: various inputs to expressions in G.
SLB: (Special Lower Bound) The Lower Bound of G, derived with fetch and cucard
We use this term instead of Lower Bound since we will use other lower bounds.
There are still three flags: Pruning (also called Group Pruning), CuCardPruning and GlobepsPruning,
with new meanings.  We plan to run benchmarks in four cases:

1. Starburst - generate all expressions [!Pruning && !CuCardPruning]
2. Group Pruning - aggressively check limits at all times [Pruning && !CuCardPruning]
aggressively check means if ( CostSoFar >= upper bound of context in G) then terminate.
3. Lower Bound Pruning - if there is no winner, then use IG's SLB in InputCost[]. [CuCardPruning].
This case assumes that the Pruning flag is on, i.e. the code forces Pruning to be true
if CuCardPruning is true.  The SLB may involve cucard, fetch, copy, etc in the lower bound.
4. Global Epsilon Pruning [GlobepsPruning].  If a plan costs <= GLOBAL_EPS, it is a winner for G.

PSEUDOCODE

On the first (and no other) execution, the code must initialize some OptimizeInputTask members.
The idea here is to get a quick lower bound for the cost of the inputs.
The only nontrivial member is InputCost; here is how to initialize it:
//Initial values of InputCost are zero in the Starburst case
For each input group IG
If (Starburst case)
      InputCost is zero
      continue
Determine property required of search in IG
If no such property, terminate this task.
call search_circle on IG with that property, infinite cost.

If case (1), no possibility of satisfying the context
      terminate this task
If search_circle returns a non-null Winner from IG, case (2)
      InputCost[IG] = cost of that winner
else if (!CuCardPruning) //Group Pruning case (since Starburst not relevant here)
      InputCost[IG] = 0
//remainder is Lower Bound Pruning case
else if there has been no previous search for ReqdProp
      InputCost[IG] = SLB
else if there has been a previous search for ReqdProp
      InputCost[IG] = max(cost of winner, IG's SLB) //This is a lower bound for IG
else
      error - previous cost failed because of property

//The rest of the code should be executed on every execution of the task.

If (Pruning && CostSoFar >= upper bound) terminate.

if (arity==0 and required property can not be satisfied)
terminate this task

//Calculate cost of remaining inputs
For each remaining (from InputNo to arity) input group IG
Call search_circle()
If Starburst case and case (1)
      error
else if case (1)
      terminate this task
else If there is a non-null Winner in IG, case (2)
      store its cost in InputCost
      if (Pruning && CostSoFar exceeds G's context's upper bound) terminate task
else if (we did not just return from OptimizeGroupTask on IG)
      //optimize this input; seek a winner for it
      push this task
      push OptimizeGroupTask for IG, using current context's cost minus CostSoFar plus InputCost[InputNo]
      terminate this task
else // we just returned from OptimizeGroupTask on IG
      Trace: This is an impossible plan
      terminate this task
InputNo++;
endFor //calculate the cost of remaining inputs

//Now all inputs have been optimized

if (CostSoFar >  G's context's upper bound)
terminate this task

//Now we know current expression satisfies current context.

if(GlobepsPruning && CostSoFar <= GLOBAL_EPS)
Make current mexpression a done winner for G
mark the current context as done
terminate this task

//Now we consider the possible states of the relevant winner in G

Search the winner's circle in G for the current task's physical property
If there is no such winner
error - the search should have initialized a winner
else If winner is done
error - we are in the midst of a search, not yet done
else If (winner is non-null and CostSoFar >= cost of this winner) || winner is null )
Replace existing winner with current mexpression and its cost, don't change done flag
Update the upper bound of the current context
*/

void OptimizeInputTask::perform() {
  PTRACE("O_INPUT performing Input " << InputNo << ", expr: " << MExpr->Dump());

  PTRACE("Context ID: " << ContextID << " , " << CONT::vc[ContextID]->Dump());
  PTRACE("Last flag is " << Last);

  // Cache local properties of G and the expression being optimized

  Operator *Op = MExpr->GetOp();  // the op of the expr
  // assert(Op->is_physical());
  // if (!Op->is_physical()) {
  //   cout << "error" << endl;
  //   abort();
  // }
  Group *LocalGroup = Ssp->GetGroup(MExpr->GetGrpID());  // Group of the MExpr

  PHYS_PROP *LocalReqdProp = CONT::vc[ContextID]->GetPhysProp();  // What prop is required
  Cost *LocalUB = CONT::vc[ContextID]->GetUpperBd();

  // Declare locals
  int IGNo;  // Input Group Number
  Group *IG;
  int input;      // index over input groups
  bool possible;  // is it possible to satisfy the required property?
                  //	Cost * CostSoFar = new Cost(0);
  Cost CostSoFar(0);

  WINNER *LocalWinner = LocalGroup->GetWinner(LocalReqdProp);  // Winner in G
  Cost Zero(0);

  // On the first (and no other) execution, code must initialize some OptimizeInputTask members.
  // The only nontrivial member is InputCost.
  if (InputNo == -1) {
    // init inputLogProp
    for (input = 0; input < arity; input++) {
      Group *InputGroup = Ssp->GetGroup(MExpr->GetInput(input));  // Group of current input
      InputLogProp[input] = InputGroup->get_log_prop();
    }

    // get the localcost of the mexpr being optimized in G
    LocalCost = Op->FindLocalCost(LocalGroup->get_log_prop(), InputLogProp);

    // For each input group IG
    for (input = 0; input < arity; input++) {
      // Initial values of InputCost are zero in the Starburst (no Pruning) case
      if (!Pruning) {
        if (!input) PTRACE("Not pruning so all InputCost elements are set to zero");
        assert(!CuCardPruning);
        InputCost[input] = &Zero;
        continue;
      }

      IGNo = MExpr->GetInput(input);
      IG = Ssp->GetGroup(IGNo);  // Group of current input

      // special case: the input is a const group - in item class
      if (IG->GetFirstLogMExpr()->GetOp()->is_const()) {
        PTRACE("Input " << input << " is a const operator so its cost is zero");
        InputCost[input] = ((CONST_OP *)IG->GetFirstLogMExpr()->GetOp())->get_cost();
        continue;
      }

      PHYS_PROP *ReqProp;
      if (Op->is_physical()) {
        // Determine property required of that input
        ReqProp = ((PhysicalOperator *)Op)->InputReqdProp(LocalReqdProp, InputLogProp[input], input, possible);

        if (!possible)  // if not possible, means no such input prop can satisfied
        {
          PTRACE("Impossible search: Bad input " << input);
          delete ReqProp;

          goto TerminateThisTask;
        }
      } else
        ReqProp = new PHYS_PROP(any);

      // call search_circle on IG with that property, infinite cost.
      bool moreSearch, SCReturn;
      Cost *INFCost = new Cost(-1);

      CONT *IGContext = new CONT(ReqProp, INFCost, false);
      SCReturn = IG->search_circle(IGContext, moreSearch);
      PTRACE("search_circle(): more search " << (moreSearch ? "" : "not") << " needed, return value is "
                                             << (SCReturn ? "true" : "false"));

      // If case (1), impossible, then terminate this task
      if (!moreSearch && !SCReturn) {
        PTRACE("Impossible search: Bad input " << input);
        delete IGContext;
        goto TerminateThisTask;
      }
      // If search_circle returns a non-null Winner from InputGroup, case (2)
      // InputCost[InputGroup] = cost of that winner
      else if (!moreSearch && SCReturn) {
        InputCost[input] = IG->GetWinner(ReqProp)->GetCost();
        assert(IG->GetWinner(ReqProp)->GetDone());
      }
      // else if (!CuCardPruning) //Group Pruning case (since Starburst not relevant here)
      // InputCost[IG] = 0
      else if (!CuCardPruning)
        InputCost[input] = &Zero;
      // remainder applies only in CuCardPruning case
      else
        InputCost[input] = IG->GetLowerBd();

      delete IGContext;
    }  // initialize some OptimizeInputTask members

    InputNo++;  // Ensure that previous code will not be executed again; begin with Input 0
  }

  // If Global Pruning and cost so far is greater than upper bound for this context, then terminate
  CostSoFar.FinalCost(LocalCost, InputCost, arity);
  if (Pruning && CostSoFar >= *LocalUB) {
    PTRACE("Expr LowerBd " << CostSoFar.Dump() << ", exceed Cond UpperBd " << LocalUB->Dump() << ",Pruning applied!");
    goto TerminateThisTask;
  }

  // Calculate the cost of remaining inputs
  for (input = InputNo; input < arity; input++) {
    // set up local variables
    IGNo = MExpr->GetInput(input);
    IG = Ssp->GetGroup(IGNo);  // Group of current input

    // special case: the input is an const_op, continue
    if (IG->GetFirstLogMExpr()
            ->GetOp()
            ->is_const()) {  // the cost of item group will not be refined, always equal to ConstGroupCost (0)
      PTRACE("Input : " << input << " is a const group");
      continue;
    }

    // generate appropriate property for search of IG
    PHYS_PROP *ReqProp;
    if (Op->is_physical()) {
      // Determine property required of that input
      ReqProp = ((PhysicalOperator *)Op)->InputReqdProp(LocalReqdProp, InputLogProp[input], input, possible);

      if (Pruning) assert(possible);  // should be possible since in the first pass, we checked it
      if (!possible) {
        delete ReqProp;
        goto TerminateThisTask;
      }
    } else
      ReqProp = new PHYS_PROP(any);

    bool moreSearch, SCReturn;
    Cost *INFCost = new Cost(-1);

    // call search_circle on IG with that property, infinite cost.
    CONT *IGContext = new CONT(ReqProp, INFCost, false);
    SCReturn = IG->search_circle(IGContext, moreSearch);

    // If case (1), impossible so terminate
    if (!moreSearch && !SCReturn) {
      PTRACE("Impossible search: Bad input " << input);
      delete IGContext;
      goto TerminateThisTask;
    }

    // else if case (2)
    else if (!moreSearch && SCReturn) {  // There is a winner with nonzero plan, in current input
      PTRACE("Found Winner for Input : " << input);
      WINNER *Winner = IG->GetWinner(ReqProp);
      assert(Winner->GetDone());

      // store its cost in InputCost[]
      InputCost[input] = Winner->GetCost();

      CostSoFar.FinalCost(LocalCost, InputCost, arity);
      // if (Pruning && CostSoFar >= upper bound) terminate this task
      if (Pruning && CostSoFar >= *LocalUB) {
        PTRACE("Expr LowerBd " << CostSoFar.Dump() << ", exceed Cond UpperBd " << LocalUB->Dump()
                               << ",Pruning applied!");
        PTRACE("This happened at group " << IGNo);

        delete IGContext;
        goto TerminateThisTask;
      }
      delete IGContext;
    }

    // Remaining cases are (3) and (4)
    else if (input != PrevInputNo)  // no winner, and we did not just return from OptimizeGroupTask
    {
      PTRACE("No Winner for Input : " << input);

      // Adjust PrevInputNo and InputNo to track progress after returning from pushes
      PrevInputNo = input;
      InputNo = input;

      // push this task
      PTasks.push(this);
      PTRACE("push myself, O_INPUT");

      // Build a context for the input group task
      // First calculate the upper bound for search of input group.
      // Upper bounds are irrelevant unless we are pruning
      Cost *InputBd = new Cost(*LocalUB);  // Start with upper bound of G's context
      if (Pruning) {
        PTRACE("LocalCost is " << LocalCost->Dump());
        CostSoFar.FinalCost(LocalCost, InputCost, arity);
        *InputBd -= CostSoFar;          // Subtract CostSoFar
        *InputBd += *InputCost[input];  // push_back IG's contribution to CostSoFar
      }

      PHYS_PROP *InputProp = new PHYS_PROP(*ReqProp);
      // update the bound in multiwinner to InputBd
      CONT *InputContext = new CONT(InputProp, InputBd, false);
      CONT::vc.push_back(InputContext);
      // Push OptimizeGroupTask
      int ContID = CONT::vc.size() - 1;
      PTRACE("push OptimizeGroupTask " << IGNo << ", " << CONT::vc[ContID]->Dump());

      PTasks.push(new OptimizeGroupTask(IG, ContID, TaskNo, true));

      // delete (void*) CostSoFar;
      delete IGContext;
      return;
    } else  // We just returned from OptimizeGroupTask on IG
    {
      // impossible plan for this context
      PTRACE("impossible plan since no winner possible at input " << InputNo);
      delete IGContext;
      goto TerminateThisTask;
    }
  }  // Calculate the cost of remaining inputs

  // If arity is zero, we need to ensure that this expression can
  // satisfy this required property.
  if (arity == 0 && LocalReqdProp->GetOrder() != any && Op->is_physical()) {
    PHYS_PROP *OutputPhysProp = ((PhysicalOperator *)Op)->FindPhysProp();
    if (!(*LocalReqdProp == *OutputPhysProp)) {
      PTRACE("physical epxr: " << MExpr->Dump() << " does not satisfy required phys_prop: " << LocalReqdProp->Dump());
      delete OutputPhysProp;

      goto TerminateThisTask;
    }
    delete OutputPhysProp;
  }

  // All inputs have been been optimized, so compute cost of the expression being optimized.

  // If we are in the root group and no plan in it has been costed
  if (!(MExpr->GetGrpID()) && !(LocalGroup->getfirstplan())) {
    OUTPUT("First Plan is costed at task " << TaskNo);
    LocalGroup->setfirstplan(true);
  }

  CostSoFar.FinalCost(LocalCost, InputCost, arity);
  PTRACE("Expression and COSTED: " << MExpr->Dump() << ": " << CostSoFar.Dump());

  // if halt, halt optimize the group when either number of plans since the
  // last winner >= HaltGrpSize*EstiGrpSize or the improvement in last HaltWinSize
  // winners is <= HaltImpr. This only works for EQJOIN
  if (Halt) {
    if (LocalGroup->GetFirstLogMExpr()->GetOp()->GetName() == ("EQJOIN")) {
      double esti_grp_size = LocalGroup->GetEstiGrpSize();
      int plan_count = LocalGroup->GetCount();
      double halt_size = esti_grp_size * HaltGrpSize / 100;

      PTRACE("Estimate Group Size is " << esti_grp_size);
      PTRACE("Number of Plans Since Last Winner is " << plan_count);
      PTRACE("Halt Size is " << halt_size);

      // if count>halt_size, we can stop here, and set the current winner as final winner
      if (plan_count >= halt_size) {
        PTRACE("Halting condition satisfied, got a final winner for this context");

        if (!LocalWinner->GetMPlan() ||            /// If there is no non-null local winner
            CostSoFar < *(LocalWinner->GetCost())  // and current expression is more expensive
        ) {
          // update the winner
          Cost *WinCost = new Cost(CostSoFar);
          LocalGroup->NewWinner(LocalReqdProp, MExpr, WinCost, true);
          // update the upperbound of the current context
          CONT::vc[ContextID]->SetUpperBound(CostSoFar);
          CONT::vc[ContextID]->done();
        } else {
          LocalWinner->SetDone(true);
          CONT::vc[ContextID]->done();
        }
        goto TerminateThisTask;
      }
    }
  }

  // Check that winner satisfies current context
  if (CostSoFar >= *LocalUB) {
    PTRACE("total cost too expensive: totalcost " << CostSoFar.Dump() << " >= upperbd " << LocalUB->Dump());
    goto TerminateThisTask;
  }

  // compare cost to current winner for this context
  // update the winner and upperbound accordingly

  if (LocalWinner->GetMPlan() &&              // If there is already a non-null local winner
      CostSoFar >= *(LocalWinner->GetCost())  // and current expression is more expensive
  )
    goto TerminateThisTask;  // Leave the non-null local winner alone
  else {
    // The expression being optimized is a new winner

    Cost *WinCost = new Cost(CostSoFar);
    LocalGroup->NewWinner(LocalReqdProp, MExpr, WinCost, Last);

    // update the upperbound of the current context
    CONT::vc[ContextID]->SetUpperBound(CostSoFar);

    PTRACE("New winner, update upperBd : " << CostSoFar.Dump());
    // delete CostSoFar;

    goto TerminateThisTask;
  }

TerminateThisTask:

  PTRACE("OptimizeInputTask this task is terminating.");
  // delete (void*) CostSoFar;

  // if this is the last task in the group, set the localwinner done=true
  if (Last) {
    LocalWinner = LocalGroup->GetWinner(LocalReqdProp);
    LocalWinner->SetDone(true);
    MExression *TempME = LocalWinner->GetMPlan();
    PTRACE("Terminate: replaced winner with " << LocalReqdProp->Dump() << ", "
                                              << (TempME ? TempME->Dump() : " nullptr ") << ", "
                                              << LocalWinner->GetCost()->Dump());
  }

  if (Last)
    // this's the last task for the group, so mark the group with completed optimizing
    Ssp->GetGroup(MExpr->GetGrpID())->set_optimized(true);

  // tasks must destroy themselves
  delete this;

}  // OptimizeInputTask::perform

string OptimizeInputTask::Dump() {
  string os;
  os = "OptimizeInputTask expr : " + MExpr->Dump() + ",";
  os += " parent task " + to_string(ParentTaskNo) + ",";

  os += " " + CONT::vc[ContextID]->Dump();
  return os;
}  // Dump

ApplyRuleTask::ApplyRuleTask(Rule *rule, MExression *mexpr, bool explore, int ContextID, int parent_task_no, bool last)
    : OptimizerTask(ContextID, parent_task_no), rule(rule), MExpr(mexpr), explore(explore), Last(last){};

ApplyRuleTask::~ApplyRuleTask() {
  if (Last) {
    Group *group = Ssp->GetGroup(MExpr->GetGrpID());
    if (!explore) {
      CONT *LocalCont = CONT::vc[ContextID];
      // What prop is required of
      PHYS_PROP *LocalReqdProp = LocalCont->GetPhysProp();
      WINNER *Winner = group->GetWinner(LocalReqdProp);

      // if (Winner) assert(!Winner->GetDone());

      // mark the winner as done
      Winner->SetDone(true);
      // this's still the last applied rule in the group,
      // so mark the group with completed optimization or exploration
      Ssp->GetGroup(MExpr->GetGrpID())->set_optimized(true);
    } else
      Ssp->GetGroup(MExpr->GetGrpID())->set_explored(true);
  }

};  // ApplyRuleTask::~ApplyRuleTask

void ApplyRuleTask::perform() {
  CONT *Context = CONT::vc[ContextID];

  PTRACE("ApplyRuleTask performing, rule: " << rule->GetName() << " expression: " << MExpr->Dump());
  PTRACE("Context ID: " << ContextID << " , " << CONT::vc[ContextID]->Dump());
  PTRACE("Last flag is " << Last);

  // if stop generating logical expression when epsilon prune is applied
  // if this context is done, stop
  // Check that this context is not done
  if (Context->is_done()) {
    PTRACE("Context: " << Context->GetPhysProp()->Dump() << " is done");
    delete this;
    return;
  }

  if (!ForGlobalEpsPruning) OptStat->FiredRule++;  // Count invocations of this task

  // main variables for the loop over all possible bindings
  BINDERY *bindery;      // Expression bindery. Used to bind MExpr to rule's original pattern
  Expression *before;    // see below
  Expression *after;     // see below
  MExression *NewMExpr;  // see below

  // Guide to closely related variables

  //	original pattern
  //     ApplyRuleTask has a rule member data.  The original pattern is
  //	   member data of that rule.  It describes (as an Expression) existing
  //	   expressions to be bound.
  //  substitute pattern
  //     from the same rule, as with original pattern.  Describes
  //	   (as an Expression) the new expression after the rule is applied.
  //  before
  //     the existing expression which is currently bound to the original
  //	   pattern by the bindery.
  //  after
  //     the new expression, in Expression form, corresponding to the substitute.
  //  NewMExpr
  //     the new expression, in MEXPR form, which has been included in the
  //     search space.

  // Loop over all Bindings of MExpr to the original pattern of the rule
  bindery = new BINDERY(MExpr, rule->GetOriginal());
  for (; bindery->advance(); delete before) {
    // There must be a Binding since advance() returned non-null.
    // Extract the bound Expression from the bindery
    before = bindery->extract_expr();
    PTRACE("new Binding is: " << endl << before->Dump());
    Bindings[rule->get_index()]++;
    // check the rule's condition function
    CONT *Cont = CONT::vc[ContextID];
    PHYS_PROP *ReqdProp = Cont->GetPhysProp();  // What prop is required of

    if (!rule->condition(before, MExpr, ContextID)) {
      PTRACE("Binding FAILS condition function, expr: " << MExpr->Dump());
      continue;  // try to find another binding
    }
    PTRACE("Binding SATISFIES condition function.  Mexpr: " << MExpr->Dump());

    Conditions[rule->get_index()]++;
    // try to derive a new substitute expression
    after = rule->next_substitute(before, ReqdProp);

    assert(after != nullptr);

    PTRACE("substitute expr is : " << endl << after->Dump());

    // include substitute in MEMO, find duplicates, etc.
    int group_no = MExpr->GetGrpID();

    NewMExpr = Ssp->CopyIn(after, group_no);

    // If substitute was already known
    if (NewMExpr == nullptr) {
      PTRACE("duplicate substitute " << after->Dump());
      delete after;  // "after" no longer used
      continue;      // try to find another substitute
    }

    PTRACE("New Mexpr is : " << NewMExpr->Dump());
    Memo_M_Exprs++;

    delete after;  // "after" no longer used

    // Give this expression the rule's mask
    NewMExpr->set_rule_mask(rule->get_mask());

    // We need to handle this case for rules like project -> nullptr,
    // by merging groups
    assert(MExpr->GetGrpID() == NewMExpr->GetGrpID());

    bool Flag = false;
    if (Last)
    // this's the last applied rule in the group,pass it to the new task
    {
      Last = false;  // turn off this, since it's no longer the last task
      Flag = true;
    }

    // follow-on tasks
    if (explore)  // optimizer is exploring, the new mexpr must be logical expr
    {
      assert(NewMExpr->GetOp()->is_logical());
      PTRACE("new task to explore new expression, pushing OptimizeExprTask exploring expr: " << NewMExpr->Dump());
      PTasks.push(new OptimizeExprTask(NewMExpr, true, ContextID, TaskNo, Flag));
    }     // optimizer is exploring
    else  // optimizer is optimizing
    {
      // for a logical op, try further transformations
      if (NewMExpr->GetOp()->is_logical()) {
        PTRACE("new task to optimize new expression,pushing OptimizeExprTask, expr: " << NewMExpr->Dump());
        PTasks.push(new OptimizeExprTask(NewMExpr, false, ContextID, TaskNo, Flag));
      }  // further transformations to optimize new expr
      else {
        // for a physical operator, optimize the inputs
        /* must be done even if op_arg -> arity == 0 in order to calculate costs */
        assert(NewMExpr->GetOp()->is_physical());

        PTRACE("new task to optimize inputs,pushing O_INPUT, epxr: " << NewMExpr->Dump());
        PTasks.push(new OptimizeInputTask(NewMExpr, ContextID, TaskNo, Flag));

      }  // for a physical operator, optimize the inputs

    }  // optimizer is optimizing

  }  // try all possible bindings

  delete bindery;

  // Mark rule vector to show that this rule has fired
  MExpr->fire_rule(rule->get_index());

  // tasks must destroy themselves
  delete this;
}

string ApplyRuleTask::Dump() {
  return "ApplyRuleTask     " + rule->Dump() + ", mexpr " + MExpr->Dump() + ", parent task " + to_string(ParentTaskNo);
}
