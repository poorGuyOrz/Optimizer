//  rules.cpp : rules implementation                Implements classes in rules.h        Columbia Optimizer Framework

#include "../header/cat.h"
#include "../header/physop.h"
#include "../header/stdafx.h"
#include "../header/tasks.h"

#define NUMOFRULES 20  // Number of elements in the enum RULELABELS in rules.h

// use to turn some rules on/off in the optimizer

int RuleVector[NUMOFRULES];

RuleSet::RuleSet() : RuleCount(NUMOFRULES) {
  int rule_count = 0;

  // if the file ends before the vectors are filled, set the rest to off
  if (rule_count < NUMOFRULES) {
    for (; rule_count < NUMOFRULES; rule_count++) RuleVector[rule_count] = 1;
  }

  rule_set.resize(RuleCount);

  // file-scan implements get
  rule_set[R_GET_TO_FILE_SCAN] = new GET_TO_FILE_SCAN();

  // filter implements select
  rule_set[R_SELECT_TO_FILTER] = new SELECT_TO_FILTER();

  // physical project implements project
  rule_set[R_P_TO_PP] = new P_TO_PP();

  // index loops join implements eqjoin
  rule_set[R_EQ_TO_LOOPS_INDEX] = new EQ_TO_LOOPS_INDEX();

  // sort merge implements eqjoin
  rule_set[R_EQ_TO_MERGE] = new EQ_TO_MERGE();

  // LOOPS JOIN implements EQJOIN
  rule_set[R_EQ_TO_LOOPS] = new EQ_TO_LOOPS();

  // sort enforcer rule
  rule_set[R_SORT_RULE] = new SORT_RULE();

  // Commute eqjoin
  rule_set[R_EQJOIN_COMMUTE] = new EQJOIN_COMMUTE();

  // Associativity of EQJOIN
  rule_set[R_EQJOIN_LTOR] = new EQJOIN_LTOR();

  // Associativity of EQJOIN
  rule_set[R_EQJOIN_RTOL] = new EQJOIN_RTOL();

  // Cesar's exchange rule
  rule_set[R_EXCHANGE] = new EXCHANGE();

  // hash_duplicates implements rm_duplicates
  rule_set[R_RM_TO_HASH_DUPLICATES] = new RM_TO_HASH_DUPLICATES();

  // hgroup_list implements agg_list
  rule_set[R_AL_TO_HGL] = new AL_TO_HGL(new AGG_OP_ARRAY(), new AGG_OP_ARRAY());

  // p_func_op implements func_op
  rule_set[R_FO_TO_PFO] = new FO_TO_PFO();

  // agg_thru_eq push agg_list below eqjoin
  rule_set[R_AGG_THRU_EQJOIN] = new AGG_THRU_EQJOIN(new AGG_OP_ARRAY(), new AGG_OP_ARRAY());

  // eq_to_bit EQJOIN to BIT_SEMIJOIN
  rule_set[R_EQ_TO_BIT] = new EQ_TO_BIT();

  // selectet_to_indexed filter
  rule_set[R_SELECT_TO_INDEXED_FILTER] = new SELECT_TO_INDEXED_FILTER();

  // project_through_select
  rule_set[R_PROJECT_THRU_SELECT] = new PROJECT_THRU_SELECT();

  // EQJOIN to HASH JOIN
  rule_set[R_EQ_TO_HASH] = new EQ_TO_HASH();

  // DUMMY to PDUMMY
  rule_set[R_DUMMY_TO_PDUMMY] = new DUMMY_TO_PDUMMY();
};  // rule set

RuleSet::~RuleSet() {
  for (auto &&rule : rule_set) delete rule;
}

string RuleSet::Dump() {
  string os;

  for (int i = 0; i < RuleCount; i++) {
    os += " " + to_string(RuleVector[i]) + " " + rule_set[i]->GetName() + "\n";
  }
  return os;
}

string RuleSet::DumpStats() {
  string os;

  os = "Rule#\tTopMatch\tBindings\tConditions\n";
  for (int i = 0; i < RuleCount; i++) {
    os += to_string(i) + "\t" + to_string(TopMatch[i]) + "\t  \t" + to_string(Bindings[i]) + "\t  \t" +
          to_string(Conditions[i]) + "\t \t" + rule_set[i]->GetName() + "\n";
  }
  return os;
}

// ====================

BINDERY::BINDERY(int group_no, Expression *original)
    : state(start),
      group_no(group_no),
      cur_expr(NULL),
      original(original),
      input(NULL),
      one_expr(false)  // try all expressions within this group
{
  assert(original);
}

BINDERY::BINDERY(MExression *expr, Expression *original)
    : state(start),
      cur_expr(expr),
      original(original),
      input(NULL),
      one_expr(true)  // restricted to this log expr
{
  group_no = expr->GetGrpID();
  assert(original);
}

BINDERY::~BINDERY() {
  if (input != NULL) {
    for (int i = 0; i < original->GetOp()->GetArity(); i++) delete input[i];
    delete[] input;
  }

};  // BINDERY::~BINDERY

Expression *BINDERY::extract_expr() {
  Expression *result;

  // If original pattern is NULL something weird is happening.
  assert(original);

  Operator *patt_op = original->GetOp();

  // Ensure that there has been a binding, so there is an
  // expression to extract.
  assert(state == valid_binding || state == finished || (patt_op->is_leaf() && state == start));

  // create leaf marked with group index
  if (patt_op->is_leaf()) {
    result = new Expression(new LeafOperator(((LeafOperator *)patt_op)->GetIndex(), group_no));
  }     // create leaf marked with group index
  else  // general invocation of new Expression
  {
    // Top operator in the new Expression will be top operator in cur_expr.
    // Get it.  (Probably could use patt_op here.)
    Operator *op_arg = cur_expr->GetOp()->Clone();

    // Need the arity of the top operator to construct inputs of new Expression
    int arity = op_arg->GetArity();

    // Inputs of new Expression can be extracted from binderys stored in
    // BINDERY::input.  Put these in the array subexpr.
    if (arity) {
      Expression **subexpr = new Expression *[arity];
      for (int input_no = 0; input_no < arity; input_no++) subexpr[input_no] = input[input_no]->extract_expr();

      // Put everything together for the result.
      result = new Expression(op_arg, subexpr);
    } else
      result = new Expression(op_arg);

  }  // general invocation of new Expression

  return result;
}

/*
    Function BINDERY::advance() walks the many trees embedded in the
    MEMO structure in order to find possible bindings.  It is called
    only by ApplyRuleTask::perform.  The walking is done with a finite
    state machine, as follows.

    State start:
        If the original pattern is a leaf, we are done.
                State = finished
                Return true
        Skip over non-logical, non-matching expressions.
                State = finished
                break
        Create a group bindery for each input and
           try to create a binding for each input.
        If successful
                State = valid_binding
                Return true
        else
                delete input binderys
                State = finished
                break


    State valid_binding:
        Increment input bindings in right-to-left order.
        If we found a next binding,
                State = valid_binding
                return true
        else
                delete input binderys
                state = finished
                break


    State finished
        If original pattern is a leaf //second time through, so we are done
           OR
           this is an expr bindery //we finished the first expression, so done
           OR
           there is no next expression
                return false
        else
                state = start
                break

*/
bool BINDERY::advance() {
#ifdef _REUSE_SIB
  /// XXXX Leave these comments alone - fix later
  Operator *patt_op = original->GetOp();
  // If the original pattern is a leaf, we will get one binding,
  //   to the entire group, then we will be done
  if (patt_op->is_leaf()) {
    switch (state) {
      case start:
        state = finished;  // failure next time, but
        return true;       // success now

      case finished:
        return false;

      default:
        assert(false);
    }
  }  // if (patt_op -> is_leaf ())

  if (!one_expr && state == start)                           // begin the group binding
  {                                                          // Search entire group for bindings
    cur_expr = Ssp->GetGroup(group_no)->GetFirstLogMExpr();  // get the first mexpr
  }

  // loop until either failure or success
  for (;;) {
    // cache some function results
    Operator *op_arg = cur_expr->GetOp();
    int arity = op_arg->GetArity();
    int input_no;

    assert(op_arg->is_logical());

    // state analysis and transitions
    switch (state) {
      case start:
        // is this expression unusable?
        if (arity != patt_op->GetArity() || !(patt_op->GetNameId() == op_arg->GetNameId())) {
          state = finished;  // try next expression
          break;
        }

        if (arity == 0)  // only the Operator, matched
        {
          state = valid_binding;
          return true;
        }  // successful bindings for the Operator without inputs
        else {
          // Create a group bindery for each input
          input = new BINDERY *[arity];
          for (input_no = 0; input_no < arity; input_no++) {
            input[input_no] = new BINDERY(cur_expr->GetInput(input_no), original->GetInput(input_no));
          }
          // Try to advance each (new) input bindery to a binding
          // a failure is failure for the expr
          for (input_no = 0; input_no < arity; input_no++)
            if (!input[input_no]->advance()) break;  // terminate this loop

          // check whether all inputs found a binding
          if (input_no == arity)  // successful!
          {
            state = valid_binding;
            return true;
          }       // successful bindings for new expression
          else {  // otherwise, failure! -- dealloc inputs
            test_delete(arity);
            state = finished;
            break;
          }
        }  // if(arity)

      case valid_binding:
        for (input_no = arity; --input_no >= 0;) {
          if (currentBind == NULL)
          // try existing inputs in right-to-left order
          // first success is overall success
          {
            if (input[input_no]->advance()) {
              for (int other_input_no = input_no; ++other_input_no < arity;) {
                // input[other_input_no] = get_first_bindery_in_list;
                // currentBind->bindery = input[other_input_no];
                input[other_input_no] = list->bindery;
                currentBind = list;
              }
              state = valid_binding;
            }
          } else {
            currentBind = currentBind->next;
            if (currentBind != NULL) {
              state = valid_binding;
              input[input_no] = currentBind->bindery;
              // input[input_no] = get_next_bindery_in_list;
              return true;
            } else
              return false;
          }
        }

        if (arity != 0 && !one_expr) {
          Node *newNode = new Node();
          BINDERY *dup = new BINDERY(this);
          newNode->bindery = dup;
          if (list == NULL)
            list = last = newNode;
          else {
            last->next = newNode;
            last = last->next;
          }
          // add_to_the_list (this bindery);
        }
        state = finished;
        break;

      case finished:
        if (one_expr || ((cur_expr = cur_expr->GetNextMExpr()) == NULL))
          return false;
        else {
          state = start;
          break;
        }

      default:
        assert(false);

    }  // state analysis and transitions
  }    // loop until either failure or success

  assert(false);  // should never terminate this loop
#else
  Operator *patt_op = original->GetOp();
  // If the original pattern is a leaf, we will get one binding,
  //   to the entire group, then we will be done
  if (patt_op->is_leaf()) {
    switch (state) {
      case start:
        state = finished;  // failure next time, but
        return true;       // success now

      case finished:
        return false;

      default:
        assert(false);
    }
  }  // if (patt_op -> is_leaf ())

  if (!one_expr && state == start)                           // begin the group binding
  {                                                          // Search entire group for bindings
    cur_expr = Ssp->GetGroup(group_no)->GetFirstLogMExpr();  // get the first mexpr
  }

  // loop until either failure or success
  for (;;) {
    // PTRACE ("advancing the cur_expr: %s", cur_expr->Dump() );

    // cache some function results
    Operator *op_arg = cur_expr->GetOp();
    int arity = op_arg->GetArity();
    int input_no;

    assert(op_arg->is_logical());

    // state analysis and transitions
    switch (state) {
      case start:

        // is this expression unusable?
        if (arity != patt_op->GetArity() || !(patt_op->GetNameId() == op_arg->GetNameId())) {
          state = finished;  // try next expression
          break;
        }

        if (arity == 0)  // only the Operator, matched
        {
          state = valid_binding;
          return true;
        }  // successful bindings for the Operator without inputs
        else {
          // Create a group bindery for each input
          input = new BINDERY *[arity];
          for (input_no = 0; input_no < arity; input_no++)
            input[input_no] = new BINDERY(cur_expr->GetInput(input_no), original->GetInput(input_no));

          // Try to advance each (new) input bindery to a binding
          // a failure is failure for the expr
          for (input_no = 0; input_no < arity; input_no++)
            if (!input[input_no]->advance()) break;  // terminate this loop

          // check whether all inputs found a binding
          if (input_no == arity)  // successful!
          {
            state = valid_binding;
            return true;
          }       // successful bindings for new expression
          else {  // otherwise, failure! -- dealloc inputs
            for (input_no = arity; --input_no >= 0;) delete input[input_no];
            delete[] input;
            input = NULL;
            state = finished;
            break;
          }
        }  // if(arity)

      case valid_binding:
        // try existing inputs in right-to-left order
        // first success is overall success
        for (input_no = arity; --input_no >= 0;) {
          if (input[input_no]->advance())
          // found one more binding
          {
            // If we have a new binding in a non-rightmost location,
            // we must create new binderys for all inputs to the
            // right of input_no, else we will not get all bindings.
            //  This is inefficient code since the each input on the
            //  right has multiple binderys created for it, and each
            //  bindery produces the same bindings as the others.
            //  The simplest example of this is the exchange rule.
            for (int other_input_no = input_no; ++other_input_no < arity;) {
              delete input[other_input_no];
              input[other_input_no] =
                  (new BINDERY(cur_expr->GetInput(other_input_no), original->GetInput(other_input_no)));

              if (!input[other_input_no]->advance())
                // Impossible since we found these bindings earlier
                assert(false);
            }

            // return overall success
            state = valid_binding;
            return true;
          }  // found one more binding
        }    // try existing inputs in right-to-left order

        // There are no more bindings to this log expr;
        //   dealloc input binderys.
        if (arity) {
          for (input_no = arity; --input_no >= 0;) delete input[input_no];
          delete[] input;
          input = NULL;
        }
        state = finished;
        break;

      case finished:

        if (one_expr || ((cur_expr = cur_expr->GetNextMExpr()) == NULL))
          return false;
        else {
          state = start;
          break;
        }

      default:
        assert(false);

    }  // state analysis and transitions
  }    // loop until either failure or success

  assert(false);  // should never terminate this loop
#endif
}  // BINDERY::advance

#ifdef _REUSE_SIB
void BINDERY::test_delete(int arity) {
  int input_no;
  for (input_no = arity; --input_no >= 0;)
    //	for (;  -- input_no >= 0; )
    delete input[input_no];
  delete[] input;
  input = NULL;
}
#endif

// Rule  Get -> File-scan
GET_TO_FILE_SCAN::GET_TO_FILE_SCAN()
    : RULE("Get->FileScan", 0, new Expression(new GET(0)), new Expression(new FILE_SCAN(0))) {
  set_index(R_GET_TO_FILE_SCAN);
};

Expression *GET_TO_FILE_SCAN::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  // create transformed expression
  return new Expression(new FILE_SCAN(((GET *)before->GetOp())->GetCollection()));
};

//  Rule  EQJOIN  -> LOOPS JOIN
EQ_TO_LOOPS::EQ_TO_LOOPS()
    : RULE(
          "EQJOIN->LOOPS_JOIN", 2,
          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1))),
          new Expression(new LOOPS_JOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                         new Expression(new LeafOperator(1)))) {
  set_index(R_EQ_TO_LOOPS);
}

Expression *EQ_TO_LOOPS::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  EQJOIN *Op = (EQJOIN *)before->GetOp();
  int size = Op->size;
  int *lattrs = CopyArray(Op->lattrs, size);
  int *rattrs = CopyArray(Op->rattrs, size);

  // create transformed expression
  Expression *result = new Expression(new LOOPS_JOIN(lattrs, rattrs, size), new Expression(*(before->GetInput(0))),
                                      new Expression(*(before->GetInput(1))));
  return result;
}  // EQ_TO_LOOPS::next_substitute
#ifdef CONDPRUNE
// Is the plan a goner because an input is group pruned?
//##ModelId=3B0C086A017E
bool EQ_TO_LOOPS::condition(Expression *before, MExression *mexpr, int ContextID) {
  Cost inputs = *(Ssp->GetGroup(mexpr->GetInput(0))->GetLowerBd());
  inputs += *(Ssp->GetGroup(mexpr->GetInput(1))->GetLowerBd());

  if (inputs >= *(CONT::vc[ContextID]->GetUpperBd())) return (false);

  return (true);
}  // EQ_TO_LOOPS::condition
#endif

// Rule  EQJOIN  -> LOOPS INDEX JOIN
EQ_TO_LOOPS_INDEX::EQ_TO_LOOPS_INDEX()
    : RULE("EQJOIN -> LOOPS_INDEX_JOIN", 1,
           new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)), new Expression(new GET(0))),
           new Expression(new LOOPS_INDEX_JOIN(0, 0, 0, 0), new Expression(new LeafOperator(0)))) {
  set_index(R_EQ_TO_LOOPS_INDEX);
}

Expression *EQ_TO_LOOPS_INDEX::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  EQJOIN *Op = (EQJOIN *)before->GetOp();
  int size = Op->size;
  int *lattrs = CopyArray(Op->lattrs, size);
  int *rattrs = CopyArray(Op->rattrs, size);

  // Get the GET logical operator in order to get the indexed collection
  GET *g = (GET *)before->GetInput(1)->GetOp();

  // create transformed expression
  result = new Expression(new LOOPS_INDEX_JOIN(lattrs, rattrs, size, g->GetCollection()),
                          new Expression(*(before->GetInput(0))));

  return (result);
}  // EQ_TO_LOOPS_INDEX::next_substitute

//  Need to check:
//	that the join is on a attribute from each table (not a multi-attribute
// 	join)
//	there is an index for the right join attribute -- need log-bulk-props
bool EQ_TO_LOOPS_INDEX::condition(Expression *before, MExression *mexpr, int ContextID) {
  // Get the GET logical operator in order to get the indexed collection
  GET *g = (GET *)before->GetInput(1)->GetOp();
  INT_ARRAY *Indices = Cat->GetIndNames(g->GetCollection());

  if (Indices == NULL) return false;

  int *rattrs = ((EQJOIN *)before->GetOp())->rattrs;
  int size = ((EQJOIN *)before->GetOp())->size;

  // Loop thru indices
  for (int i = 0; i < Indices->size(); i++)

    if (size == 1 && Cat->GetIndProp((*Indices)[i])->Keys->ContainKey(rattrs[0])) return (true);

  return (false);

}  // EQ_TO_LOOPS_INDEX::condition

// Rule  EQJOIN  -> MERGE JOIN
EQ_TO_MERGE::EQ_TO_MERGE()
    : RULE(
          "EQJOIN -> MERGE_JOIN", 2,
          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1))),
          new Expression(new MERGE_JOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                         new Expression(new LeafOperator(1)))) {
  set_index(R_EQ_TO_MERGE);
}

int EQ_TO_MERGE::promise(Operator *op_arg, int ContextID) {
  // if the merge-join attributes set is empty, don't fire this rule
  int result = (((EQJOIN *)op_arg)->size == 0) ? 0 : MERGE_PROMISE;

  return (result);
}  // EQ_TO_MERGE::promise

Expression *EQ_TO_MERGE::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  EQJOIN *Op = (EQJOIN *)before->GetOp();
  int size = Op->size;
  int *lattrs = CopyArray(Op->lattrs, size);
  int *rattrs = CopyArray(Op->rattrs, size);

  // create transformed expression
  result = new Expression(new MERGE_JOIN(lattrs, rattrs, size), new Expression(*(before->GetInput(0))),
                          new Expression(*(before->GetInput(1))));

  return (result);
}

#ifdef CONDPRUNE
// Is the plan a goner because an input is group pruned?
//##ModelId=3B0C086A0232
bool EQ_TO_MERGE::condition(Expression *before, MExression *mexpr, int ContextID) {
  Cost inputs = *(Ssp->GetGroup(mexpr->GetInput(0))->GetLowerBd());
  inputs += *(Ssp->GetGroup(mexpr->GetInput(1))->GetLowerBd());

  if (inputs >= *(CONT::vc[ContextID]->GetUpperBd())) return (false);

  return (true);
}  // EQ_TO_MERGE::condition
#endif

/*
  Rule  EQJOIN  -> HASH JOIN
  ====  ======  == ===== ====
*/

//##ModelId=3B0C086A02B4
EQ_TO_HASH::EQ_TO_HASH()
    : RULE(
          "EQJOIN->HASH_JOIN", 2,
          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1))),
          new Expression(new HASH_JOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                         new Expression(new LeafOperator(1)))) {
  // set rule index
  set_index(R_EQ_TO_HASH);
}  // EQ_TO_HASH::EQ_TO_HASH

//##ModelId=3B0C086A02B5
int EQ_TO_HASH::promise(Operator *op_arg, int ContextID) {
  // if the hash-join attributes set is empty, don't fire this rule
  int result = (((EQJOIN *)op_arg)->size == 0) ? 0 : HASH_PROMISE;

  return (result);
}  // EQ_TO_HASH::promise

//##ModelId=3B0C086A02C8
Expression *EQ_TO_HASH::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  EQJOIN *Op = (EQJOIN *)before->GetOp();
  int size = Op->size;
  int *lattrs = CopyArray(Op->lattrs, size);
  int *rattrs = CopyArray(Op->rattrs, size);

  // create transformed expression
  result = new Expression(new HASH_JOIN(lattrs, rattrs, size), new Expression(*(before->GetInput(0))),
                          new Expression(*(before->GetInput(1))));

  return (result);
}  // EQ_TO_HASH::next_substitute

/*
  Rule  EQJOIN(A,B)  -> EQJOIN(B,A)
  ====  ===========  == ===========
*/
//##ModelId=3B0C086A03CE
EQJOIN_COMMUTE::EQJOIN_COMMUTE()
    : RULE(
          "EQJOIN_COMMUTE", 2,
          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1))),
          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(1)),
                         new Expression(new LeafOperator(0)))) {
  // set rule mask and index
  set_index(R_EQJOIN_COMMUTE);
  set_mask(1 << R_EQJOIN_COMMUTE | 1 << R_EQJOIN_LTOR | 1 << R_EQJOIN_RTOL | 1 << R_EXCHANGE);

}  // EQJOIN_COMMUTE::EQJOIN_COMMUTE

//##ModelId=3B0C086A03D8
Expression *EQJOIN_COMMUTE::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  // lattrs and rattrs
  EQJOIN *Op = (EQJOIN *)before->GetOp();
  int size = Op->size;
  int *lattrs = CopyArray(Op->lattrs, size);
  int *rattrs = CopyArray(Op->rattrs, size);

  // create transformed expression
  Expression *result = new Expression(new EQJOIN(rattrs, lattrs, size),  // reverse l and r
                                      new Expression(*(before->GetInput(1))), new Expression(*(before->GetInput(0))));

  return result;
}  // EQJOIN_COMMUTE::next_substitute

/*
  Rule  EQJOIN (AB) C -> EQJOIN A (BC)
  ====  ============= == =============
*/
// assoc of join left to right
EQJOIN_LTOR::EQJOIN_LTOR()
    : RULE("EQJOIN_LTOR", 3,
           new Expression(new EQJOIN(0, 0, 0),
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(0)),  // A
                                         new Expression(new LeafOperator(1))   // B
                                         ),
                          new Expression(new LeafOperator(2))  // C
                          ),                                   // original pattern
           new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(1)),
                                         new Expression(new LeafOperator(2))))  // substitute
      )

{
  // set rule mask and index
  set_index(R_EQJOIN_LTOR);
  set_mask(1 << R_EQJOIN_LTOR | 1 << R_EQJOIN_RTOL | 1 << R_EXCHANGE);

}  // EQJOIN_LTOR::EQJOIN_LTOR

Expression *EQJOIN_LTOR::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  /*
   * Join numbering convention:
   *          1  2             2  1		join number
   * EQJOIN (AxB)xC -> EQJOIN Ax(BxC)		original -> substitute patterns
   */
  // from upper (second) join
  EQJOIN *Op2 = (EQJOIN *)before->GetOp();
  int size2 = Op2->size;
  int *lattrs2 = Op2->lattrs;  // equal attr's from left
  int *rattrs2 = Op2->rattrs;

  // from lower (first) join
  EQJOIN *Op1 = (EQJOIN *)before->GetInput(0)->GetOp();
  int size1 = Op1->size;
  int *lattrs1 = Op1->lattrs;  // equal attr's from left
  int *rattrs1 = Op1->rattrs;

  //	Calculate the new join conditions
  int i;
  // for new upper (second) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs2 = new int[size1 + size2];
  int *nlattrs2 = new int[size1 + size2];
  // first copy lattrs1 to nlattrs2
  for (i = 0; i < size1; i++) {
    nlattrs2[i] = lattrs1[i];
    nrattrs2[i] = rattrs1[i];
  }
  int nsize2 = size1;

  // for new lower (first) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs1 = new int[size2];
  int *nlattrs1 = new int[size2];
  int nsize1 = 0;

  //	B's schema determines new joining conditions.  Get it.
  Expression *AB = before->GetInput(0);
  LeafOperator *B = (LeafOperator *)(AB->GetInput(1)->GetOp());
  int group_no = B->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  Schema *Bs_schema = ((LOG_COLL_PROP *)(group->get_log_prop()))->schema;

  // See where second join predicates of antecedent go
  for (i = 0; i < size2; i++) {
    if (Bs_schema->InSchema(lattrs2[i])) {
      // lattrs2[i] is in B:  put this eq condition in first join
      nlattrs1[nsize1] = lattrs2[i];
      nrattrs1[nsize1] = rattrs2[i];
      nsize1++;
    } else {
      // lattrs2[i] is in A:  put this eq condition in second join
      nlattrs2[nsize2] = lattrs2[i];
      nrattrs2[nsize2] = rattrs2[i];
      nsize2++;
    }
  }

  // Check that each join is legal

  // Get C's schema
  LeafOperator *C = (LeafOperator *)(before->GetInput(1)->GetOp());
  group_no = C->GetGroup();
  group = Ssp->GetGroup(group_no);
  Schema *Cs_schema = ((LOG_COLL_PROP *)group->get_log_prop())->schema;

  // check that first join is legal
  for (i = 0; i < nsize1; i++) {
    assert(Bs_schema->InSchema(nlattrs1[i]));
    assert(Cs_schema->InSchema(nrattrs1[i]));
  }

  //  Get A's schema
  LeafOperator *A = (LeafOperator *)(AB->GetInput(0)->GetOp());
  group_no = A->GetGroup();
  group = Ssp->GetGroup(group_no);
  Schema *As_schema = ((LOG_COLL_PROP *)group->get_log_prop())->schema;

  // Check (mostly) that second join is legal
  for (i = 0; i < nsize2; i++) {
    assert(As_schema->InSchema(nlattrs2[i]));
  }

  // create transformed expression
  Expression *result = new Expression(
      new EQJOIN(nlattrs2, nrattrs2, nsize2), new Expression(*(before->GetInput(0)->GetInput(0))),
      new Expression(new EQJOIN(nlattrs1, nrattrs1, nsize1), new Expression(*(before->GetInput(0)->GetInput(1))),
                     new Expression(*(before->GetInput(1)))));
  return result;

}  // EQJOIN_LTOR::next_substitute

/*
 *	Experimental Heuristic code to try to limit rule explosion
 *		This may eliminate useful join orders
 *		Global variable NoCart determines whether
 *		this condition function is ENABLED or DISABLED
 */

// If we allow a non-Cartesian product to go to a Cartesian product, return true
// If we do not allow a non-Cartesian product to go to a Cartesian product
// Need to check:
//	a. Whether original 2 joins       contained a cartesian product
//	b. Whether the new  2 joins would contain   a cartesian product
// Condition is:
//	a || !b
//
//##ModelId=3B0C086B00B7
bool EQJOIN_LTOR::condition(Expression *before, MExression *mexpr, int ContextID) {
#ifdef NOCART
  PTRACE("%s\n", "NOCART is On");
  EQJOIN *Op2 = (EQJOIN *)before->GetOp();
  int size2 = Op2->size;
  int *lattrs2 = Op2->lattrs;  // equal attr's from left
  int *rattrs2 = Op2->rattrs;

  // from lower (first) join
  EQJOIN *Op1 = (EQJOIN *)before->GetInput(0)->GetOp();
  int size1 = Op1->size;
  int *lattrs1 = Op1->lattrs;  // equal attr's from left
  int *rattrs1 = Op1->rattrs;

  //	Calculate the new join conditions
  int i;
  // for new upper (second) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs2 = new int[size1 + size2];
  int *nlattrs2 = new int[size1 + size2];
  // first copy lattrs1 to nlattrs2
  for (i = 0; i < size1; i++) {
    nlattrs2[i] = lattrs1[i];
    nrattrs2[i] = rattrs1[i];
  }
  int nsize2 = size1;

  // for new lower (first) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs1 = new int[size2];
  int *nlattrs1 = new int[size2];
  int nsize1 = 0;

  //	B's schema determines new joining conditions.  Get it.
  Expression *AB = before->GetInput(0);
  LeafOperator *B = (LeafOperator *)(AB->GetInput(1)->GetOp());
  int group_no = B->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  Schema *Bs_schema = ((LOG_COLL_PROP *)(group->get_log_prop()))->schema;

  // See where second join predicates of antecedent go
  for (i = 0; i < size2; i++) {
    if (Bs_schema->InSchema(lattrs2[i])) {
      // lattrs2[i] is in B:  put this eq condition in first join
      nlattrs1[nsize1] = lattrs2[i];
      nrattrs1[nsize1] = rattrs2[i];
      nsize1++;
    } else {
      // lattrs2[i] is in A:  put this eq condition in second join
      nlattrs2[nsize2] = lattrs2[i];
      nrattrs2[nsize2] = rattrs2[i];
      nsize2++;
    }
  }

  // Condition is that either one of the original joins had
  // a cartesian product -- or that neither of the resulting
  // joins has a cartesian product
  if (size1 * size2 != 0 && nsize1 * nsize2 == 0) return false;
  return true;
#endif
  return true;

}  // EQJOIN_LTOR::condition

/*
Rule  EQJOIN A (BC) -> EQJOIN (AB) C
====  ============= == =============
Right to Left Associativity
*/
//##ModelId=3B0C086B016C
EQJOIN_RTOL::EQJOIN_RTOL()
    : RULE("EQJOIN_RTOL", 3,
           new Expression(new EQJOIN(0, 0, 0),
                          new Expression(new LeafOperator(0)),  // A
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(1)),  // B
                                         new Expression(new LeafOperator(2))   // C
                                         )),                                   // original pattern
           new Expression(new EQJOIN(0, 0, 0),
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(0)),  // A
                                         new Expression(new LeafOperator(1))   // B
                                         ),
                          new Expression(new LeafOperator(2))  // C
                          )                                    // substitute
      ) {
  // set rule mask and index
  set_index(R_EQJOIN_RTOL);
  set_mask(1 << R_EQJOIN_LTOR | 1 << R_EQJOIN_RTOL | 1 << R_EXCHANGE);
}  // EQJOIN_RTOL::EQJOIN_RTOL

Expression *EQJOIN_RTOL::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  /*
   * Join numbering convention:
   *         2  1               1  2		1 is the first join
   * EQJOIN Ax(BxC) -> EQJOIN (AxB)xC
   */
  // from upper (second) join
  EQJOIN *Op2 = (EQJOIN *)before->GetOp();
  int size2 = Op2->size;
  int *lattrs2 = Op2->lattrs;  // equal attr's from left
  int *rattrs2 = Op2->rattrs;

  // from lower (first) join
  EQJOIN *Op1 = (EQJOIN *)before->GetInput(1)->GetOp();
  int size1 = Op1->size;
  int *lattrs1 = Op1->lattrs;  // equal attr's from left
  int *rattrs1 = Op1->rattrs;

  //	Calculate the new join conditions
  int i;
  // for new upper (second) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs2 = new int[size1 + size2];
  int *nlattrs2 = new int[size1 + size2];
  // first copy lattrs1 to nlattrs2
  for (i = 0; i < size1; i++) {
    nlattrs2[i] = lattrs1[i];
    nrattrs2[i] = rattrs1[i];
  }
  int nsize2 = size1;

  // for new lower (first) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs1 = new int[size2];
  int *nlattrs1 = new int[size2];
  int nsize1 = 0;

  // Get schema for B
  Expression *BC = before->GetInput(1);
  LeafOperator *B = (LeafOperator *)(BC->GetInput(0)->GetOp());
  int group_no = B->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  LOG_PROP *LogProp = group->get_log_prop();
  Schema *Bs_schema = ((LOG_COLL_PROP *)LogProp)->schema;

  // See where second join predicates of antecedent go
  for (i = 0; i < size2; i++) {
    if (Bs_schema->InSchema(rattrs2[i])) {
      // lattrs2[i] is in B:  put this eq condition in first join
      nlattrs1[nsize1] = lattrs2[i];
      nrattrs1[nsize1] = rattrs2[i];
      nsize1++;
    } else {
      // lattrs2[i] is in A:  put this eq condition in second join
      nlattrs2[nsize2] = lattrs2[i];
      nrattrs2[nsize2] = rattrs2[i];
      nsize2++;
    }
  }

  // check (mostly) that first join is legal
  for (i = 0; i < nsize1; i++) {
    assert(Bs_schema->InSchema(nrattrs1[i]));
    //"Bad B join atts"
  }

  // Get C's schema
  LeafOperator *C = (LeafOperator *)(BC->GetInput(1)->GetOp());
  group_no = C->GetGroup();
  group = Ssp->GetGroup(group_no);
  LogProp = group->get_log_prop();
  Schema *Cs_schema = ((LOG_COLL_PROP *)LogProp)->schema;

  // Check (mostly) that second join is legal
  for (i = 0; i < nsize2; i++) {
    assert(Cs_schema->InSchema(nrattrs2[i]));
    //"Bad C join atts"
  }

  // create transformed expression
  Expression *result =
      new Expression(new EQJOIN(nlattrs2, nrattrs2, nsize2),  // args reversed
                     new Expression(new EQJOIN(nlattrs1, nrattrs1, nsize1), new Expression(*(before->GetInput(0))),
                                    new Expression(*(before->GetInput(1)->GetInput(0)))),
                     new Expression(*(before->GetInput(1)->GetInput(1))));
  return result;
}  // EQJOIN_RTOL::next_substitute

/*
 *	Very experimental code to try to limit rule explosion
 *		This may eliminate very useful join orders
 *		Go to the very end of this function to see if
 *		this condition function is ENABLED or DISABLED
 */
// If we allow a non-Cartesian product to go to a Cartesian product, return true
// If we do not allow a non-Cartesian product to go to a Cartesian product
// Need to check:
//	a. Whether original 2 joins       contained a cartesian product
//	b. Whether the new  2 joins would contain   a cartesian product
// Condition is:
//	a || !b
//
//##ModelId=3B0C086B0181
bool EQJOIN_RTOL::condition(Expression *before, MExression *mexpr, int ContextID) {
#ifdef NOCART
  PTRACE("%s\n", "NOCART is On");
  EQJOIN *Op2 = (EQJOIN *)before->GetOp();
  int size2 = Op2->size;
  int *lattrs2 = Op2->lattrs;  // equal attr's from left
  int *rattrs2 = Op2->rattrs;

  // from lower (first) join
  EQJOIN *Op1 = (EQJOIN *)before->GetInput(0)->GetOp();
  int size1 = Op1->size;
  int *lattrs1 = Op1->lattrs;  // equal attr's from left
  int *rattrs1 = Op1->rattrs;

  //	Calculate the new join conditions
  int i;
  // for new upper (second) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs2 = new int[size1 + size2];
  int *nlattrs2 = new int[size1 + size2];
  // first copy lattrs1 to nlattrs2
  for (i = 0; i < size1; i++) {
    nlattrs2[i] = lattrs1[i];
    nrattrs2[i] = rattrs1[i];
  }
  int nsize2 = size1;

  // for new lower (first) join
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs1 = new int[size2];
  int *nlattrs1 = new int[size2];
  int nsize1 = 0;

  //	B's schema determines new joining conditions.  Get it.
  Expression *AB = before->GetInput(0);
  LeafOperator *B = (LeafOperator *)(AB->GetInput(1)->GetOp());
  int group_no = B->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  Schema *Bs_schema = ((LOG_COLL_PROP *)(group->get_log_prop()))->Schema;

  // See where second join predicates of antecedent go
  for (i = 0; i < size2; i++) {
    if (Bs_schema->InSchema(lattrs2[i])) {
      // lattrs2[i] is in B:  put this eq condition in first join
      nlattrs1[nsize1] = lattrs2[i];
      nrattrs1[nsize1] = rattrs2[i];
      nsize1++;
    } else {
      // lattrs2[i] is in A:  put this eq condition in second join
      nlattrs2[nsize2] = lattrs2[i];
      nrattrs2[nsize2] = rattrs2[i];
      nsize2++;
    }
  }

  // Condition is that either one of the original joins had
  // a cartesian product -- or that neither of the resulting
  // joins has a cartesian product
  if (size1 * size2 != 0 && nsize1 * nsize2 == 0) return false;
  return true;
#endif
  return true;
}  // EQJOIN_RTOL::condition

/*
        Cesar's EXCHANGE rule: (AxB)x(CxD) -> (AxC)x(BxD)
          ====  ============= =
*/
EXCHANGE::EXCHANGE()
    : RULE("EXCHANGE", 4,
           new Expression(new EQJOIN(0, 0, 0),
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(0)),   // A
                                         new Expression(new LeafOperator(1))),  // B
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(2)),    // C
                                         new Expression(new LeafOperator(3)))),  // D

           new Expression(new EQJOIN(0, 0, 0),
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(0)),   // A
                                         new Expression(new LeafOperator(2))),  // C
                          new Expression(new EQJOIN(0, 0, 0),
                                         new Expression(new LeafOperator(1)),    // B
                                         new Expression(new LeafOperator(3)))))  // D
{
  // set rule mask and index
  set_index(R_EXCHANGE);
  set_mask(1 << R_EQJOIN_COMMUTE | 1 << R_EQJOIN_LTOR | 1 << R_EQJOIN_RTOL | 1 << R_EXCHANGE);
}  // EXCHANGE::EXCHANGE

//##ModelId=3B0C086B0249
Expression *EXCHANGE::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  /*
   * Join numbering convention:
   *   2  1  3        2  1  3
   * (AxB)x(CxD) -> (AxC)x(BxD)
   */

  // from join 1
  EQJOIN *Op1 = (EQJOIN *)before->GetOp();
  int size1 = Op1->size;
  int *lattrs1 = Op1->lattrs;
  int *rattrs1 = Op1->rattrs;

  // from join 2
  EQJOIN *Op2 = (EQJOIN *)before->GetInput(0)->GetOp();
  int size2 = Op2->size;
  int *lattrs2 = Op2->lattrs;
  int *rattrs2 = Op2->rattrs;

  // from join 3
  EQJOIN *Op3 = (EQJOIN *)before->GetInput(1)->GetOp();
  int size3 = Op3->size;
  int *lattrs3 = Op3->lattrs;
  int *rattrs3 = Op3->rattrs;

  /*
      First we factor lattrs1 into its A and B parts,
          lattrs1 = lattrs1A U lattrs1B
      and similarly
          rattrs1 = rattrs1C U rattrs1D

      The new attribute sets for join 1 are given by
          nlattrs1 = lattrs2 U lattrs3 U rattrs1C
          nrattrs1 = rattrs2 U rattrs3 U lattrs1B

      Those for joins 2, 3 are trickier - see the code below.  Approximately,
          nlattrs2 = lattrs1A, nrattrs2 = rattrs1C
          nlattrs3 = lattrs1B, nrattrs3 = rattrs1D
  */
  // for new join 1
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs1 = new int[size1 + size2 + size3];
  int *nlattrs1 = new int[size1 + size2 + size3];
  int i;
  // Compute nlattrs1, nrattrs1 incrementally, first lattrs2 U lattrs3
  for (i = 0; i < size2; i++) {
    nlattrs1[i] = lattrs2[i];
    nrattrs1[i] = rattrs2[i];
  }
  for (i = 0; i < size3; i++) {
    nlattrs1[i + size2] = lattrs3[i];
    nrattrs1[i + size2] = rattrs3[i];
  }
  int nsize1 = size2 + size3;

  // for new join 2
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs2 = new int[size1];
  int *nlattrs2 = new int[size1];
  int nsize2 = 0;

  // for new join 3
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs3 = new int[size1];
  int *nlattrs3 = new int[size1];
  int nsize3 = 0;

  // Get schema for A
  Expression *AB = before->GetInput(0);
  LeafOperator *AA = (LeafOperator *)AB->GetInput(0)->GetOp();
  int group_no = AA->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  LOG_PROP *log_prop = group->get_log_prop();
  Schema *AAA = ((LOG_COLL_PROP *)log_prop)->schema;

  // Get schema for C
  Expression *CD = before->GetInput(1);
  LeafOperator *CC = (LeafOperator *)CD->GetInput(0)->GetOp();
  group_no = CC->GetGroup();
  group = Ssp->GetGroup(group_no);
  log_prop = group->get_log_prop();
  Schema *CCC = ((LOG_COLL_PROP *)log_prop)->schema;

  // If a pair of attributes in (lattrs1,rattrs1), is in (A,C),
  // then the pair should be in (nlattrs2,nrattrs2).
  // If the pair is in (B,D), put it in in (nlattrs3,nrattrs3).
  // If it is in (A, D), put it in (nlattrs1, nrattrs1).
  // Otherwise go to (nlattrs1, nrattrs1), but reversed.

  for (i = 0; i < size1; i++) {
    int lkey = lattrs1[i];
    int rkey = rattrs1[i];
    if (AAA->InSchema(lkey) &&  // lattrs1 in A
        CCC->InSchema(rkey)     // rattrs1 in C
    ) {
      nlattrs2[nsize2] = lkey;  // Add to nlattrs2
      nrattrs2[nsize2] = rkey;  // Add to nrattrs2
      nsize2++;
    } else if (!(AAA->InSchema(lkey)) &&  // lattrs1 in B
               !(CCC->InSchema(rkey))     // rattrs1 in D
    ) {
      nlattrs3[nsize3] = lkey;  // Add to nlattrs3
      nrattrs3[nsize3] = rkey;  // Add to nrattrs3
      nsize3++;
    } else if ((AAA->InSchema(lkey)) &&  // lattrs1 in A
               !(CCC->InSchema(rkey))    // rattrs1 in D
    ) {
      nlattrs1[nsize1] = lkey;  // add to nlattrs1
      nrattrs1[nsize1] = rkey;  // add to nrattrs1
      nsize1++;
    } else  // lattrs1 in B and rattrs1 in C
    {
      nlattrs1[nsize1] = rkey;  // add to nlattrs1, reversed
      nrattrs1[nsize1] = lkey;  // add to nrattrs1, reversed
      nsize1++;
    }
  }

  // create transformed expression
  Expression *result = new Expression(new EQJOIN(nlattrs1, nrattrs1, nsize1),
                                      new Expression(new EQJOIN(nlattrs2, nrattrs2, nsize2),
                                                     new Expression(*(before->GetInput(0)->GetInput(0))),  // A
                                                     new Expression(*(before->GetInput(1)->GetInput(0)))   // C
                                                     ),
                                      new Expression(new EQJOIN(nlattrs3, nrattrs3, nsize3),
                                                     new Expression(*(before->GetInput(0)->GetInput(1))),  // B
                                                     new Expression(*(before->GetInput(1)->GetInput(1)))   // D
                                                     ));

  return result;
}  // EXCHANGE::next_substitute

/*
 *	Very experimental code to try to limit rule explosion
 *		This may eliminate very useful join orders
 *		Go to the very end of this function to see if
 *		this condition function is ENABLED or DISABLED
 */

// If we allow a non-Cartesian product to go to a Cartesian product, return true
// If we do not allow a non-Cartesian product to go to a Cartesian product
// Need to check:
//	a. Whether original 3 joins       contained a cartesian product
//	b. Whether the new  3 joins would contain   a cartesian product
// Condition is:
//	a || !b
//
//##ModelId=3B0C086B025C
bool EXCHANGE::condition(Expression *before, MExression *mexpr, int ContextID) {
#ifdef NOCART
  /*
   * Join numbering convention:
   *   2  1  3        2  1  3
   * (AxB)x(CxD) -> (AxC)x(BxD)
   */

  // from join 1
  EQJOIN *Op1 = (EQJOIN *)before->GetOp();
  int size1 = Op1->size;
  int *lattrs1 = Op1->lattrs;
  int *rattrs1 = Op1->rattrs;

  // from join 2
  EQJOIN *Op2 = (EQJOIN *)before->GetInput(0)->GetOp();
  int size2 = Op2->size;
  int *lattrs2 = Op2->lattrs;
  int *rattrs2 = Op2->rattrs;

  // from join 3
  EQJOIN *Op3 = (EQJOIN *)before->GetInput(1)->GetOp();
  int size3 = Op3->size;
  int *lattrs3 = Op3->lattrs;
  int *rattrs3 = Op3->rattrs;

  // for new join 1
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs1 = new int[size1 + size2 + size3];
  int *nlattrs1 = new int[size1 + size2 + size3];
  int i;
  // Compute nlattrs1, nrattrs1 incrementally, first lattrs2 U lattrs3
  for (i = 0; i < size2; i++) {
    nlattrs1[i] = lattrs2[i];
    nrattrs1[i] = rattrs2[i];
  }
  for (i = 0; i < size3; i++) {
    nlattrs1[i + size2] = lattrs3[i];
    nrattrs1[i + size2] = rattrs3[i];
  }
  int nsize1 = size2 + size3;

  // for new join 2
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs2 = new int[size1];
  int *nlattrs2 = new int[size1];
  int nsize2 = 0;

  // for new join 3
  // first allocate bigger size, then set the new right size after addkeys
  int *nrattrs3 = new int[size1];
  int *nlattrs3 = new int[size1];
  int nsize3 = 0;

  // Get schema for A
  Expression *AB = before->GetInput(0);
  LeafOperator *AA = (LeafOperator *)AB->GetInput(0)->GetOp();
  int group_no = AA->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  LOG_PROP *log_prop = group->get_log_prop();
  Schema *AAA = ((LOG_COLL_PROP *)log_prop)->Schema;

  // Get schema for C
  Expression *CD = before->GetInput(1);
  LeafOperator *CC = (LeafOperator *)CD->GetInput(0)->GetOp();
  group_no = CC->GetGroup();
  group = Ssp->GetGroup(group_no);
  log_prop = group->get_log_prop();
  Schema *CCC = ((LOG_COLL_PROP *)log_prop)->Schema;

  // If a pair of attributes in (lattrs1,rattrs1), is in (A,C),
  // then the pair should be in (nlattrs2,nrattrs2).
  // If the pair is in (B,D), put it in in (nlattrs3,nrattrs3).
  // If it is in (A, D), put it in (nlattrs1, nrattrs1).
  // Otherwise go to (nlattrs1, nrattrs1), but reversed.

  for (i = 0; i < size1; i++) {
    int lkey = lattrs1[i];
    int rkey = rattrs1[i];
    if (AAA->InSchema(lkey) &&  // lattrs1 in A
        CCC->InSchema(rkey)     // rattrs1 in C
    ) {
      nlattrs2[nsize2] = lkey;  // Add to nlattrs2
      nrattrs2[nsize2] = rkey;  // Add to nrattrs2
      nsize2++;
    } else if (!(AAA->InSchema(lkey)) &&  // lattrs1 in B
               !(CCC->InSchema(rkey))     // rattrs1 in D
    ) {
      nlattrs3[nsize3] = lkey;  // Add to nlattrs3
      nrattrs3[nsize3] = rkey;  // Add to nrattrs3
      nsize3++;
    } else if ((AAA->InSchema(lkey)) &&  // lattrs1 in A
               !(CCC->InSchema(rkey))    // rattrs1 in D
    ) {
      nlattrs1[nsize1] = lkey;  // add to nlattrs1
      nrattrs1[nsize1] = rkey;  // add to nrattrs1
      nsize1++;
    } else  // lattrs1 in B and rattrs1 in C
    {
      nlattrs1[nsize1] = rkey;  // add to nlattrs1, reversed
      nrattrs1[nsize1] = lkey;  // add to nrattrs1, reversed
      nsize1++;
    }
  }
  // Condition is that either one of the original joins had
  // a cartesian product -- or that neither of the resulting
  // joins has a cartesian product
  if (size1 * size2 * size3 != 0 && nsize1 * nsize2 * nsize3 == 0) return false;
  return true;
#endif
  return true;
}  // EXCHANGE::condition

/*
  Rule  SELECT  -> FILTER
  ====  ======  == ======
*/

//##ModelId=3B0C086B0361
SELECT_TO_FILTER::SELECT_TO_FILTER()
    : RULE("SELECT -> FILTER", 2,
           new Expression(new SELECT,
                          new Expression(new LeafOperator(0)),  // table
                          new Expression(new LeafOperator(1))   // predicate
                          ),
           new Expression(new FILTER, new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1)))) {
  // set rule index
  set_index(R_SELECT_TO_FILTER);
}  // SELECT_TO_FILTER::SELECT_TO_FILTER

//##ModelId=3B0C086B036A
Expression *SELECT_TO_FILTER::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  // create transformed expression
  result = new Expression(new FILTER, new Expression(*(before->GetInput(0))), new Expression(*(before->GetInput(1))));

  return (result);
}  // SELECT_TO_FILTER::next_substitute

/*
    Rule  PROJ -> P PROJ
    ====  ==== == =-====
*/

//##ModelId=3B0C086B02DE
P_TO_PP::P_TO_PP()
    : RULE("PROJECT -> P_PROJECT", 1, new Expression(new PROJECT(0, 0), new Expression(new LeafOperator(0))),
           new Expression(new P_PROJECT(0, 0), new Expression(new LeafOperator(0)))) {
  // set rule index
  set_index(R_P_TO_PP);
}  // P_TO_PP::P_TO_PP

Expression *P_TO_PP::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  PROJECT *Op = (PROJECT *)before->GetOp();
  int size = Op->size;
  int *attrs = CopyArray(Op->attrs, size);

  // Get input's schema
  LeafOperator *A = (LeafOperator *)(before->GetInput(0)->GetOp());
  int group_no = A->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  LOG_PROP *LogProp = group->get_log_prop();
  Schema *As_schema = ((LOG_COLL_PROP *)LogProp)->schema;

  /*
   * Check that project list is in incoming schema
   */
  for (int i = 0; i < size; i++) assert(As_schema->InSchema(attrs[i]));

  // create transformed expression
  result = new Expression(new P_PROJECT(attrs, size), new Expression(*(before->GetInput(0))));

  return (result);
}  // P_TO_PP::next_substitute

/*
    Rule  Sort enforcer
    ====  ============
*/

//##ModelId=3B0C086C0037
SORT_RULE::SORT_RULE()
    : RULE("SORT enforcer", 1, new Expression(new LeafOperator(0)),
           new Expression(new QSORT(),  // bogus should this be oby?
                          new Expression(new LeafOperator(0)))) {
  // set rule index
  set_index(R_SORT_RULE);
}  // SORT_RULE::SORT_RULE

//##ModelId=3B0C086C004C
Expression *SORT_RULE::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  // create transformed expression
  Expression *result = new Expression(new QSORT(), new Expression(*before));
  return (result);
}

//##ModelId=3B0C086C0041
int SORT_RULE::promise(Operator *op_arg, int ContextID) {
  CONT *Cont = CONT::vc[ContextID];
  PHYS_PROP *ReqdProp = Cont->GetPhysProp();  // What prop is required of

  int result = (ReqdProp->GetOrder() == any) ? 0 : SORT_PROMISE;

  return (result);
}  // SORT_RULE::promise

/*
    Rule  RM_DUPLICATES  -> HASH_DUPLICATES
    ====  ======  == ======
*/

//##ModelId=3B0C086C00EB
RM_TO_HASH_DUPLICATES::RM_TO_HASH_DUPLICATES()
    : RULE("RM_DUPLICATES  -> HASH_DUPLICATES", 1,
           new Expression(new RM_DUPLICATES(),
                          new Expression(new LeafOperator(0))  // input table
                          ),
           new Expression(new HASH_DUPLICATES(), new Expression(new LeafOperator(0)))) {
  // set rule index
  set_index(R_RM_TO_HASH_DUPLICATES);
}  // RM_TO_HASH_DUPLICATES::RM_TO_HASH_DUPLICATES

//##ModelId=3B0C086C00F5
Expression *RM_TO_HASH_DUPLICATES::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  // create transformed expression
  result = new Expression(new HASH_DUPLICATES(), new Expression(*(before->GetInput(0))));

  return (result);
}  // RM_TO_HASH_DUPLICATES::next_substitute

/*
    Rule  AGG_LIST  -> HGROUP_LIST
    ====  ======  == ======
*/

//##ModelId=3B0C086C018C
AL_TO_HGL::AL_TO_HGL(AGG_OP_ARRAY *list1, AGG_OP_ARRAY *list2)
    : RULE("AGG_LIST  -> HGROUP_LIST", 1,
           new Expression(new AGG_LIST(0, 0, list1),
                          new Expression(new LeafOperator(0))  // input table
                          ),
           new Expression(new HGROUP_LIST(0, 0, list2), new Expression(new LeafOperator(0)))) {
  // set rule index
  set_index(R_AL_TO_HGL);
}  // AL_TO_HGL::AL_TO_HGL

//##ModelId=3B0C086C0196
Expression *AL_TO_HGL::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  AGG_LIST *Op = (AGG_LIST *)before->GetOp();
  int size = Op->GbySize;
  int *attrs = CopyArray(Op->GbyAtts, size);

  int i;
  AGG_OP_ARRAY *agg_ops = new AGG_OP_ARRAY;
  agg_ops->resize(Op->AggOps->size());
  for (i = 0; i < Op->AggOps->size(); i++) {
    (*agg_ops)[i] = new AGG_OP(*(*Op->AggOps)[i]);
  }

  // create transformed expression
  result = new Expression(new HGROUP_LIST(attrs, size, agg_ops), new Expression(*(before->GetInput(0))));

  return (result);
}  // AL_TO_HGL::next_substitute

/*
    Rule  FUNC_OP  -> P_FUNC_OP
    ====  ======  == ======
*/

//##ModelId=3B0C086C0235
FO_TO_PFO::FO_TO_PFO()
    : RULE("FUNC_OP  -> P_FUNC_OP", 1,
           new Expression(new FUNC_OP("", 0, 0),
                          new Expression(new LeafOperator(0))  // input table
                          ),
           new Expression(new P_FUNC_OP("", 0, 0), new Expression(new LeafOperator(0)))) {
  // set rule index
  set_index(R_FO_TO_PFO);
}  // FO_TO_PFO::FO_TO_PFO

//##ModelId=3B0C086C0240
Expression *FO_TO_PFO::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  FUNC_OP *Op = (FUNC_OP *)before->GetOp();
  int size = Op->AttsSize;
  int *attrs = CopyArray(Op->Atts, size);

  // create transformed expression
  result = new Expression(new P_FUNC_OP(Op->RangeVar, attrs, size), new Expression(*(before->GetInput(0))));

  return (result);
}  // FO_TO_PFO::next_substitute

/*
    Rule  AGGREGATE EQJOIN  -> JOIN AGGREGATE
    ====  ======  == ===== ====
*/

//##ModelId=3B0C086C0308
AGG_THRU_EQJOIN::AGG_THRU_EQJOIN(AGG_OP_ARRAY *list1, AGG_OP_ARRAY *list2)
    : RULE("AGGREGATE EQJOIN -> JOIN AGGREGATE", 2,
           new Expression(new AGG_LIST(0, 0, list1),
                          new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                                         new Expression(new LeafOperator(1)))),
           new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                          new Expression(new AGG_LIST(0, 0, list2), new Expression(new LeafOperator(1))))) {
  // set rule index
  set_index(R_AGG_THRU_EQJOIN);
}  // AGG_THRU_EQ::AGG_THRU_EQ

//##ModelId=3B0C086C031B
Expression *AGG_THRU_EQJOIN::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;
  int i;
  // get GbyAtts and AggOps
  AGG_LIST *agg_op = (AGG_LIST *)before->GetOp();
  int *gby_atts = CopyArray(agg_op->GbyAtts, agg_op->GbySize);
  AGG_OP_ARRAY *agg_ops = new AGG_OP_ARRAY;
  agg_ops->resize(agg_op->AggOps->size());
  for (i = 0; i < agg_op->AggOps->size(); i++) {
    (*agg_ops)[i] = new AGG_OP(*(*agg_op->AggOps)[i]);
  }

  // EQJOIN is the only input to AGG_LIST
  EQJOIN *Op = (EQJOIN *)before->GetInput(0)->GetOp();
  int size = Op->size;
  int *lattrs = CopyArray(Op->lattrs, size);
  int *rattrs = CopyArray(Op->rattrs, size);
  // get the schema of the right input
  LeafOperator *r_op = (LeafOperator *)before->GetInput(0)->GetInput(1)->GetOp();
  int r_gid = r_op->GetGroup();
  Group *r_group = Ssp->GetGroup(r_gid);
  LOG_COLL_PROP *r_prop = (LOG_COLL_PROP *)r_group->get_log_prop();
  Schema *right_schema = r_prop->schema;

  // new_gby = (gby^right_schema) u ratts
  int new_gby_size = 0;
  int index = 0;

  for (i = 0; i < agg_op->GbySize; i++) {
    if (right_schema->InSchema(gby_atts[i])) new_gby_size++;
  }
  new_gby_size += size;
  int *new_gby_atts = new int[new_gby_size];
  for (i = 0; i < size; i++, index++) {
    new_gby_atts[index] = rattrs[i];
  }
  for (i = 0; i < agg_op->GbySize; i++, index++) {
    if (right_schema->InSchema(gby_atts[i])) new_gby_atts[index] = gby_atts[i];
  }

  delete[] gby_atts;
  // create transformed expression
  result = new Expression(new EQJOIN(lattrs, rattrs, size), new Expression(*(before->GetInput(0)->GetInput(0))),
                          new Expression(new AGG_LIST(new_gby_atts, new_gby_size, agg_ops),
                                         new Expression(*(before->GetInput(0)->GetInput(1)))));

  return (result);
}  // AGG_THRU_EQJOIN::next_substitute

/* Conditions:
        0. only joining on a single equality condition - fix later
        1. All attributes used in aggregates must be in the right schema
        2. The attributes referenced in the EQJOIN predicate are in the group by list
        3. The EQJOIN is a foreign key join:
        Conditon 3 is checked by if lattrs contains candidate_key of left input
*/
//##ModelId=3B0C086C0325
bool AGG_THRU_EQJOIN::condition(Expression *before, MExression *mexpr, int ContextID) {
  // get attributes used in aggregates, and groupby attributes
  AGG_LIST *agg_op = (AGG_LIST *)before->GetOp();
  int *agg_atts = agg_op->FlattenedAtts;
  int *gby_atts = agg_op->GbyAtts;

  // EQJOIN is the only input to AGG_LIST
  EQJOIN *Op = (EQJOIN *)before->GetInput(0)->GetOp();
  int size = Op->size;
  int *lattrs = Op->lattrs;
  int *rattrs = Op->rattrs;
  // get the schema of the right input
  LeafOperator *r_op = (LeafOperator *)before->GetInput(0)->GetInput(1)->GetOp();
  int r_gid = r_op->GetGroup();
  Group *r_group = Ssp->GetGroup(r_gid);
  LOG_COLL_PROP *r_prop = (LOG_COLL_PROP *)r_group->get_log_prop();
  Schema *right_schema = r_prop->schema;
  // get candidatekey of the left input
  LeafOperator *l_op = (LeafOperator *)before->GetInput(0)->GetInput(0)->GetOp();
  int l_gid = l_op->GetGroup();
  Group *l_group = Ssp->GetGroup(l_gid);
  LOG_COLL_PROP *l_prop = (LOG_COLL_PROP *)l_group->get_log_prop();
  KEYS_SET *l_cand_key = l_prop->CandidateKey;

  int i, j;
  // check condition 1 agg_atts in right schema
  for (i = 0; i < agg_op->FAttsSize; i++) {
    if (!right_schema->InSchema(agg_atts[i])) return (false);
  }
  // check condition 2 ratts in gby or latts in gby
  for (i = 0; i < size; i++) {
    for (j = 0; j < agg_op->GbySize; j++)
      if (rattrs[i] == gby_atts[j]) break;
    if (j == agg_op->GbySize) break;
  }
  if (i < size) {
    // continue to check lattrs
    for (i = 0; i < size; i++) {
      for (j = 0; j < agg_op->GbySize; j++)
        if (lattrs[i] == gby_atts[j]) break;
      if (j == agg_op->GbySize) break;
    }
  }
  if (i < size) return (false);

  // check condition 3, left_cand_key is contained in lattrs
  if (l_cand_key->IsSubSet(lattrs, size)) return (true);

  return (false);
}  // AGG_THRU_EQJOIN::condition

/*
    Rule  EQJOIN  -> BIT INDEX JOIN
    ====  ======  == ===== ====
*/

//##ModelId=3B0C086D0010
EQ_TO_BIT::EQ_TO_BIT()
    : RULE("EQJOIN -> BIT_JOIN", 2,
           new Expression(new EQJOIN(0, 0, 0), new Expression(new LeafOperator(0)),
                          new Expression(new SELECT,
                                         new Expression(new GET(0)),          // table
                                         new Expression(new LeafOperator(1))  // predicate
                                         )),
           new Expression(new BIT_JOIN(0, 0, 0, 0), new Expression(new LeafOperator(0)),
                          new Expression(new LeafOperator(1)))) {
  // set rule index
  set_index(R_EQ_TO_BIT);
}  // EQ_TO_BIT::EQ_TO_BIT

//##ModelId=3B0C086D001A
Expression *EQ_TO_BIT::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  // EQJOIN is the only input to PROJECT
  EQJOIN *EqOp = (EQJOIN *)before->GetOp();
  int size = EqOp->size;
  int *lattrs = CopyArray(EqOp->lattrs, size);
  int *rattrs = CopyArray(EqOp->rattrs, size);

  // Get is the left input to SELECT
  GET *GetOp = (GET *)before->GetInput(1)->GetInput(0)->GetOp();

  result = new Expression(new BIT_JOIN(lattrs, rattrs, size, GetOp->GetCollection()),
                          new Expression(*(before->GetInput(0))), new Expression(*(before->GetInput(1)->GetInput(1))));

  return (result);
}  // AGG_THRU_EQJOIN::next_substitute

/* Conditions:
      1. only joining on a single equality condition
      2. index_attr candidate key of LEAF(0)
      3. EQJOIN is a foreign key join, with Lattr the foreign key (rattr is the candidate
      key of relation 'collection')
      4.   predicate.free_variables == bit_attr('collection')
      AND:
      predicate=bit_pred('collection')
 */
//##ModelId=3B0C086D0024
bool EQ_TO_BIT::condition(Expression *before, MExression *mexpr, int ContextID) {
  EQJOIN *EqOp = (EQJOIN *)before->GetOp();
  int size = EqOp->size;
  int *lattrs = EqOp->lattrs;
  int *rattrs = EqOp->rattrs;

  // get left input to eqjoin
  //  Get schema for LEAF(0)
  LeafOperator *LEAF0 = (LeafOperator *)(before->GetInput(0)->GetOp());
  int group_no = LEAF0->GetGroup();
  Group *group = Ssp->GetGroup(group_no);
  LOG_PROP *LogProp = group->get_log_prop();
  KEYS_SET *l_cand_key = ((LOG_COLL_PROP *)LogProp)->CandidateKey;

  // Get is the left input to SELECT
  GET *GetOp = (GET *)before->GetInput(1)->GetInput(0)->GetOp();
  // get the candidate_keys and bit index
  int CollId = GetOp->GetCollection();
  // get candidate keys on 'collection'
  KEYS_SET *candidate_key = Cat->GetCollProp(CollId)->CandidateKey;
  // get the bitindex names of the 'collection
  INT_ARRAY *BitIndices = Cat->GetBitIndNames(CollId);

  // Get is the predicate of SELECT
  LeafOperator *PredOp = (LeafOperator *)before->GetInput(1)->GetInput(1)->GetOp();
  int Pred_GID = PredOp->GetGroup();
  Group *leaf_group = Ssp->GetGroup(Pred_GID);
  LOG_PROP *leaf_prop = leaf_group->get_log_prop();
  KEYS_SET pred_freevar = ((LOG_ITEM_PROP *)leaf_prop)->FreeVars;

  // conditon 1: only joining on a single equality condition
  if (size == 1 && l_cand_key && BitIndices) {
    // check all the BitIndex
    for (int CurrBitIndex = 0; CurrBitIndex < BitIndices->size(); CurrBitIndex++) {
      int index_attr = Cat->GetBitIndProp((*BitIndices)[CurrBitIndex])->IndexAttr;
      KEYS_SET *bit_attrs = Cat->GetBitIndProp((*BitIndices)[CurrBitIndex])->BitAttr;

      // condition2: index_attr is a candidate key of LEAF(0)
      if (l_cand_key->GetSize() != 1 || ((*l_cand_key)[0]) != index_attr) continue;

      // conditon 3:the EQJOIN is a foreign key join, with lattr the foreign key
      if (!candidate_key->IsSubSet(rattrs, 1)) {
        return (false);
      }

      // conditon 4:   predicate.free_variables == bit_attr('collection')
      //			   AND:
      //				predicate=bit_pred('collection')
      if (bit_attrs) {
        // assume all the value are covered
        if ((pred_freevar) == (*bit_attrs)) return (true);
      }
    }
  }

  return (false);
}
// EQ_TO_BIT::condition

/*
Rule  SELECT  -> INDEXED_FILTER
====  ======  == ===== ====
*/

//##ModelId=3B0C086D00F6
SELECT_TO_INDEXED_FILTER::SELECT_TO_INDEXED_FILTER()
    : RULE("SELECT -> INDEXED_FILTER", 1,
           new Expression(new SELECT,
                          new Expression(new GET(0)),          // table
                          new Expression(new LeafOperator(0))  // predicate
                          ),
           new Expression(new INDEXED_FILTER(0), new Expression(new LeafOperator(0)))) {
  // set rule index
  set_index(R_SELECT_TO_INDEXED_FILTER);
}  // SELECT_TO_INDEXED_FILTER::SELECT_TO_INDEXED_FILTER

//##ModelId=3B0C086D0100
Expression *SELECT_TO_INDEXED_FILTER::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;

  // create transformed expression
  result = new Expression(new INDEXED_FILTER(((GET *)before->GetInput(0)->GetOp())->GetCollection()),
                          new Expression(*(before->GetInput(1))));
  return result;
}  // GET_TO_FILE_SCAN::next_substitute

//      Need to check:
//      Predicate only has one free variable
//      Index exists for the free variable
//##ModelId=3B0C086D010A
bool SELECT_TO_INDEXED_FILTER::condition(Expression *before, MExression *mexpr, int ContextID) {
  // get the index list of the GET collection
  int CollId = ((GET *)before->GetInput(0)->GetOp())->GetCollection();
  INT_ARRAY *Indices = Cat->GetIndNames(CollId);

  // get the predicate free variables
  LeafOperator *Pred = (LeafOperator *)(before->GetInput(1)->GetOp());
  int GrpNo = Pred->GetGroup();
  Group *PredGrp = Ssp->GetGroup(GrpNo);
  LOG_PROP *log_prop = PredGrp->get_log_prop();
  KEYS_SET FreeVar = ((LOG_ITEM_PROP *)log_prop)->FreeVars;

  if (Indices && FreeVar.GetSize() == 1) {
    for (int i = 0; i < Indices->size(); i++) {
      if ((*Cat->GetIndProp((*Indices)[i])->Keys) == FreeVar) return (true);
    }
  }

  return (false);
}

/* ============================================================ */
/*
Rule  PROJ SEL -> SEL PROJ
====  ==== === == === ====
*/
//##ModelId=3B0C086D01B5
PROJECT_THRU_SELECT::PROJECT_THRU_SELECT()
    : RULE("PROJECT_THRU_SELECT", 2,
           new Expression(new PROJECT(0, 0),
                          new Expression(new SELECT,  // table
                                         new Expression(new LeafOperator(0)),
                                         new Expression(new LeafOperator(1))  // predicate
                                         )),
           new Expression(
               new PROJECT(0, 0),
               new Expression(new SELECT, new Expression(new PROJECT(0, 0), new Expression(new LeafOperator(0))),
                              new Expression(new LeafOperator(1))))) {
  // set rule index
  set_index(R_PROJECT_THRU_SELECT);
  set_mask(1 << R_PROJECT_THRU_SELECT);
}  // PROJECT_THRU_SELECT::PROJECT_THRU_SELECT()

Expression *PROJECT_THRU_SELECT::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  Expression *result;
  int *top_pattrs;
  int top_size, pattr_size;
  int *pattrs;

  // Get the projection attributes
  top_pattrs = ((PROJECT *)before->GetOp())->attrs;
  top_size = ((PROJECT *)before->GetOp())->size;

  // Get the select predicate free variables spfv
  LeafOperator *Pred = (LeafOperator *)(before->GetInput(0)->GetInput(1)->GetOp());
  int GrpNo = Pred->GetGroup();
  Group *PredGrp = Ssp->GetGroup(GrpNo);
  LOG_PROP *log_prop = PredGrp->get_log_prop();
  KEYS_SET FreeVar = ((LOG_ITEM_PROP *)log_prop)->FreeVars;

  // merge free variables of predicate into the project list
  KEYS_SET *temp = new KEYS_SET(top_pattrs, top_size);
  temp->Merge(FreeVar);
  pattrs = temp->CopyOut();
  pattr_size = temp->GetSize();

  // create transformed expression
  result =
      new Expression(new PROJECT(CopyArray(top_pattrs, top_size), top_size),
                     new Expression(new SELECT,
                                    // new Expression (before->GetInput(0)->GetOp(),	//select
                                    new Expression(new PROJECT(pattrs, pattr_size),
                                                   new Expression((*(before->GetInput(0)->GetInput(0))))  // LEAF(0)
                                                   ),
                                    new Expression((*(before->GetInput(0)->GetInput(1))))  // LEAF(1)
                                    ));
  return result;
}  // PROJECT_THRU_SELECT::next_substitute

/*
    Rule  DUMMY  -> PDUMMY
    ====  =====  == ======
*/

//##ModelId=3B0C086D026A
DUMMY_TO_PDUMMY::DUMMY_TO_PDUMMY()
    : RULE("DUMMY->PDUMMY", 2,
           new Expression(new DUMMY(), new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1))),
           new Expression(new PDUMMY(), new Expression(new LeafOperator(0)), new Expression(new LeafOperator(1))

                              )) {
  // set rule index
  set_index(R_DUMMY_TO_PDUMMY);

}  // DUMMY_TO_PDUMMY::DUMMY_TO_PDUMMY

//##ModelId=3B0C086D0273
Expression *DUMMY_TO_PDUMMY::next_substitute(Expression *before, PHYS_PROP *ReqdProp) {
  DUMMY *Op = (DUMMY *)before->GetOp();

  // create transformed expression
  Expression *result =
      new Expression(new PDUMMY(), new Expression(*(before->GetInput(0))), new Expression(*(before->GetInput(1))));
  return result;
}  // DUMMY_TO_PDUMMY::next_substitute
