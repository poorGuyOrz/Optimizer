// RULES.H - Base classes and actual rules

#pragma once
#include "../header/ssp.h"

class BINDERY;
class Rule;
class RuleSet;
class GET_TO_FILE_SCAN;
class SELECT_TO_FILTER;
class P_TO_PP;
class EQ_TO_LOOPS_INDEX;
class EQ_TO_MERGE;
class EQ_TO_HASH;
class EQ_TO_LOOPS;
class SORT_RULE;
class EQJOIN_COMMUTE;
class EQJOIN_LTOR;
class EQJOIN_RTOL;
class EXCHANGE;
class RM_TO_HASH_DUPLICATES;
class AL_TO_HGL;
class FO_TO_PFO;
class AGG_THRU_EQJOIN;
class SELECT_TO_INDEXED_FILTER;
class DUMMY_TO_PDUMMY;

/*
This enum list is used for the rule bit vector.  It must be consistent with NUMOFRULES
and rule_set and with the rule set file read from disk. If there are more than 32 rules,
put only logical to logical transformations in the rule vector.
*/
typedef enum RULELABELS {
  R_GET_TO_FILE_SCAN,
  R_SELECT_TO_FILTER,
  R_P_TO_PP,
  R_EQ_TO_LOOPS_INDEX,
  R_EQ_TO_MERGE,
  R_EQ_TO_LOOPS,
  R_SORT_RULE,
  R_EQJOIN_COMMUTE,
  R_EQJOIN_LTOR,
  R_EQJOIN_RTOL,
  R_EXCHANGE,
  R_RM_TO_HASH_DUPLICATES,
  R_AL_TO_HGL,
  R_FO_TO_PFO,
  R_AGG_THRU_EQJOIN,
  R_EQ_TO_BIT,
  R_SELECT_TO_INDEXED_FILTER,
  R_PROJECT_THRU_SELECT,
  R_EQ_TO_HASH,
  R_DUMMY_TO_PDUMMY,
  R_END,
} RULELABELS;

enum class RuleType : uint32_t {
  // Transformation rules (logical -> logical)
  INNER_JOIN_COMMUTE = 0,
  INNER_JOIN_ASSOCIATE,

  // Don't move this one
  LogicalPhysicalDelimiter,

  // Implementation rules (logical -> physical)
  GET_TO_DUMMY_SCAN,
  GET_TO_SEQ_SCAN,
  GET_TO_INDEX_SCAN,
  QUERY_DERIVED_GET_TO_PHYSICAL,
  EXTERNAL_FILE_GET_TO_PHYSICAL,
  DELETE_TO_PHYSICAL,
  UPDATE_TO_PHYSICAL,
  INSERT_TO_PHYSICAL,
  INSERT_SELECT_TO_PHYSICAL,
  AGGREGATE_TO_HASH_AGGREGATE,
  AGGREGATE_TO_PLAIN_AGGREGATE,
  INNER_JOIN_TO_INDEX_JOIN,
  INNER_JOIN_TO_NL_JOIN,
  SEMI_JOIN_TO_HASH_JOIN,
  INNER_JOIN_TO_HASH_JOIN,
  LEFT_JOIN_TO_HASH_JOIN,
  IMPLEMENT_DISTINCT,
  IMPLEMENT_LIMIT,
  EXPORT_EXTERNAL_FILE_TO_PHYSICAL,
  ANALYZE_TO_PHYSICAL,
  CTESCAN_TO_PHYSICAL,
  CTESCAN_TO_PHYSICAL_EMPTY,

  // Create/Drop
  CREATE_DATABASE_TO_PHYSICAL,
  CREATE_FUNCTION_TO_PHYSICAL,
  CREATE_INDEX_TO_PHYSICAL,
  CREATE_TABLE_TO_PHYSICAL,
  CREATE_NAMESPACE_TO_PHYSICAL,
  CREATE_TRIGGER_TO_PHYSICAL,
  CREATE_VIEW_TO_PHYSICAL,
  DROP_DATABASE_TO_PHYSICAL,
  DROP_TABLE_TO_PHYSICAL,
  DROP_INDEX_TO_PHYSICAL,
  DROP_NAMESPACE_TO_PHYSICAL,
  DROP_TRIGGER_TO_PHYSICAL,
  DROP_VIEW_TO_PHYSICAL,

  // Don't move this one
  RewriteDelimiter,

  // Rewrite rules (logical -> logical)
  PUSH_FILTER_THROUGH_JOIN,
  PUSH_FILTER_THROUGH_AGGREGATION,
  COMBINE_CONSECUTIVE_FILTER,
  EMBED_FILTER_INTO_GET,
  EMBED_FILTER_INTO_CTE_SCAN,
  MARK_JOIN_GET_TO_INNER_JOIN,
  SINGLE_JOIN_GET_TO_INNER_JOIN,
  DEPENDENT_JOIN_GET_TO_INNER_JOIN,
  MARK_JOIN_INNER_JOIN_TO_INNER_JOIN,
  MARK_JOIN_FILTER_TO_INNER_JOIN,
  PULL_FILTER_THROUGH_MARK_JOIN,
  PULL_FILTER_THROUGH_AGGREGATION,
  UNION_WITH_RECURSIVE_CTE,
  QUERY_DERIVED_GET_ON_TABLE_FREE_SCAN,

  // Place holder to generate number of rules compile time
  NUM_RULES
};

class RuleSet {
 private:
  vector<Rule *> rule_set;

 public:
  int RuleCount;  // size of rule_set

  RuleSet();
  ~RuleSet();

  string Dump();
  string DumpStats();

  // return the Rule in the order Set
  inline Rule *operator[](int n) { return rule_set[n]; }
};

/*
    BINDERY
    ========
   At the heart of every rule- (transform-) based optimizer is the concept of
   a rule.  A rule includes two patterns, called the Original and Substitute
   patterns.  For example, the Left to Right (LTOR) Associative Rule includes the
   following patterns, where L(i) denotes the LeafOperator with index i (L(i) is
   essentially a multiexpression indexed by i):
                Original: (L(1) join L(2)) join L(3)
                Substitute: L(1) join (L(2) join L(3))

   One of the first steps in applying this rule is to find a multiexpression in the search
   space which binds to the Original pattern.  This is the task of objects in the BINDERY
   class.

   Suppose the multiexpression
                [ G7 join G4 ] join G10,
   where Gi is the group with int i and [ X ] denotes the group containing X, is in
   the optimizer's search space.  Then there is a binding of the Original pattern to
   this multiexpression, given by binding L(1) to G7, L(2) to G4 and L(3) to G10. An
   object in the BINDERY class, called a bindery, will, over its lifetime, produce
   all such bindings.

   In order to produce a binding, a bindery must spawn one bindery for each input subgroup.
   For example, consider a bindery for the LTOR associativity rule.  It will spawn a
   bindery for the left input, which will seek all bindings to the pattern L(1) join L(2)
   and a bindery for the right input, which will seek all bindings to the pattern  L(3).
   In our example the right bindery will find only one binding, to the right input group G10.  The left bindery will
   typically find many bindings, However, the left bindery will find one binding for each join in the left input group,
   for example one binding for G7 join G4 and one for G4 join G7.

   Is a binding made to a multiexpression or to an expression?  In our example, a binding
   is sought for to the multiexpression [G7 join G4] join G10 .  However, the bindings
   which are produced are to expressions, such as (G7 join G4) join G10 and
   (G4 join G7) join G10.  Those two are distinct as expressions but would be identical
   as multiexpressions.

   BINDERY objects (binderys) are of two types.  Expression binderys bind the Original
   pattern to only one multi-expression in a group.  A rule first creates an expression
   bindery.  This expression bindery then spawns one group bindery for each input group,
   as explained above. Group binderys bind to all expressions in a group.  See
   ApplyRuleTask::perform  for details .

   Because Columbia and its predecessors apply rules only to logical expressions, binderys
   bind logical operators only.

   A bindery may produce several bindings, as mentioned above.  Thus a bindery may go
   through several stages: start, then loop over several valid bindings, then finish.

*/

class BINDERY {
 private:
  Expression *original;  // bind with this original pattern
  MExression *cur_expr;  // bind the original pattern to this multi-expression
  bool one_expr;         // Is this an expression bindery?
  int group_no;          // group no of the cur_expr

  typedef enum BINDERY_STATE {
    start,          // This is a new MExpression
    valid_binding,  // A binding was found.
    finished,       // Finished with this expression
  } BINDERY_STATE;

  BINDERY_STATE state;

  BINDERY **input;  // binderys for input expr's

 public:
  // Create an Expression bindery
  BINDERY(MExression *expr, Expression *original);

  // Create a Group bindery
  BINDERY(int group_no, Expression *original);

  ~BINDERY();

  // advance() requests a bindery to produce its next binding, if one
  // exists.  This may cause the state of the bindery to change.
  // advance() returns true if a binding has been found.
  bool advance();

  // If a valid binding has been found, then return the bound Expression.
  Expression *extract_expr();

  // print the name of the current state
  string print_state();

};  // BINDERY

/*
  Rules
  =====
      See BINDERY above.

  A rule is defined primarily by its original and substitute patterns.
  For example, the LTOR join associative rule has these patterns,
  in which L(i) stands for Leaf operator i:
         Original pattern: (L(1) join L(2)) join L(3)
       Substitute pattern: L(1) join (L(2) join L(3))

  The original and substitute patterns describe how to produce new
      multi-expressions in the search space.  The production of these new
      multi-expressions is done by ApplyRuleTask::perform(), in two parts:
  First a BINDERY object produces a binding of the original pattern to an Expression
  in the search space.  Then next_substitute() produces the new expression,
  which is integrated into the search space by SearchSpace::include().

  A rule is called an implementation rule if the root operator of its substitute
      pattern is a physical operator, for example the rule GET_TO_SCAN.

  Columbia and its ancestors use only rules for which all operators in the
      original pattern are logical, and for which all operators in the substitute
      pattern are logical except perhaps the root operator.

  In OptimizeExprTask::perform(), the optimizer decides which rules to push onto
  the PTASK stack.  It uses top_match() to check whether the root operator
  of the original pattern of a rule matches the root operator of the
      current multiexpression.

  The method OptimizeExprTask::perform() must decide the order in which rules
  are pushed onto the PTASK stack.  For this purpose it uses promise().
  A promise value of 0 or less means do not schedule this rule here.
  Higher promise values mean schedule this rule earlier.
  By default, an implementation  rule has a promise of 2 and others
  a promise of 1.  Notice that a rule's promise is evaluated before
  inputs of the top-most operator are expanded (searched, transformed)
  and matched against the rule's pattern; the promise value is used
  to decide whether or not to explore those inputs.
  promise() is used for both exploring and optimizing, though for
  exploration it is currently irrelevant.

  When a rule is applied (in ApplyRuleTask::perform), and after a binding
  is found, the method condition() is invoked to determine whether
  the rule actually applies.  condition() has available to it the entire
  Expression matched to the rule's original pattern.  For example, the rule which
  pushes a select below a join requires a condition about compatibitily
  of schemas.  This condition cannot be checked until after the binding,
  since schemas of input groups are only available from the binding.

  The check() method verifies that a rule seems internally consistent,
  i.e., that a rule's given cardinality is consistent with its given
  pattern, and that pattern and substitute satisfy other
  requirements.  These requirements are:
  - leaves are numbered 0, 1, 2, ..., (arity-1)
  - all leaf numbers up to (arity-1) are used in the original pattern
  - each leaf number is used exactly once in the original pattern
  - the substitute uses only leaf numbers in the original pattern
  - (each leaf number may appear 0, 1, 2, or more times)
  - all operators in the original pattern are logical operators
  - all operators except the root in the substitute pattern are logical
*/

#define FILESCAN_PROMISE 5
#define SORT_PROMISE 6
#define MERGE_PROMISE 4
#define HASH_PROMISE 4
#define PHYS_PROMISE 3
#define LOG_PROMISE 1
#define ASSOC_PROMISE 2

class Rule {
 private:
  Expression *original;    // original pattern to match
  Expression *substitute;  // replacement for original pattern
                           //"substitute" is used ONLY to tell if the rule is logical or physical,
                           // and, by check(), for consistency checks.  Its pattern is represented
                           // in the method next_substitute()

  int arity;  // number of leaf operators in original pattern.

  string name;

  BIT_VECTOR mask;  // Which rules to turn off in "after" expression
  int index;        // index in the rule set

 public:
  Rule(string name, int arity, Expression *original, Expression *substitute)
      : name(name), arity(arity), mask(0), original(original), substitute(substitute){};

  virtual ~Rule() {
    delete original;
    delete substitute;
  };

  inline string GetName() { return (name); };
  inline Expression *GetOriginal() { return (original); };
  inline Expression *GetSubstitute() { return (substitute); };

  bool top_match(Operator *op_arg) {
    assert(op_arg->is_logical());  // to make sure never OptimizeExprTask a physcial mexpr

    // if original is a leaf, it represents a group, so it always matches
    if (original->GetOp()->is_leaf()) return true;

    // otherwise, the original pattern should have a logical root op
    return (((LogicalOperator *)(original->GetOp()))->OpMatch((LogicalOperator *)op_arg));
  };

  // default value is 1.0, resulting in exhaustive search
  virtual int promise(Operator *op_arg, int ContextID) {
    return (substitute->GetOp()->is_physical() ? PHYS_PROMISE : LOG_PROMISE);
  };

  // Does before satisfy this rule's condition, if we are using
  // context for the search?  mexpr is the multi-expression bound to
  // before, probably mexpr is not needed.
  // Default value is TRUE, i.e., rule applies
  virtual bool condition(Expression *before, MExression *mexpr, int ContextID) { return true; };

  // Given an expression which is a binding (before), this
  //  returns the next substitute form (after) of the rule.
  virtual Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp) = 0;

  bool check();  // check that original & subst. patterns are legal

  inline int get_index() { return (index); };       // get the rule's index in the rule set
  inline BIT_VECTOR get_mask() { return (mask); };  // get the rule's mask

  inline void set_index(int i) { index = i; };
  inline void set_mask(BIT_VECTOR v) { mask = v; };

  string Dump() { return "rule : " + name; };

  // if not stop generating logical expression when epsilon pruning is applied
  // need these to identify the substitue
};

//  GET TO FILE SCAN
class GET_TO_FILE_SCAN : public Rule {
 public:
  GET_TO_FILE_SCAN();
  ~GET_TO_FILE_SCAN(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  int promise(Operator *op_arg, int ContextID) { return FILESCAN_PROMISE; };
};

//  EQJOIN to LOOPS Rule
class EQ_TO_LOOPS : public Rule {
 public:
  EQ_TO_LOOPS();
  ~EQ_TO_LOOPS(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);

};  // EQ_TO_LOOPS

//  EQJOIN to Merge Join Rule
class EQ_TO_MERGE : public Rule {
 public:
  EQ_TO_MERGE();
  int promise(Operator *op_arg, int ContextID);
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};  // EQ_TO_MERGE

// EQJOIN to Hash Join Rule
class EQ_TO_HASH : public Rule {
 public:
  EQ_TO_HASH();
  int promise(Operator *op_arg, int ContextID);
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};  // EQ_TO_HASH

/*
   ============================================================
   EQJOIN to LOOPS Index Rule
   ============================================================
*/
class EQ_TO_LOOPS_INDEX : public Rule {
 public:
  EQ_TO_LOOPS_INDEX();
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};  // EQ_TO_LOOPS_INDEX

/*
============================================================
EQJOIN Commutativity Rule
============================================================
*/
//##ModelId=3B0C086A03C3
class EQJOIN_COMMUTE : public Rule {
 public:
  //##ModelId=3B0C086A03CE
  EQJOIN_COMMUTE();
  //##ModelId=3B0C086A03D7
  ~EQJOIN_COMMUTE(){};
  //##ModelId=3B0C086A03D8
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};  // EQJOIN_COMMUTE

/*
   ============================================================
   EQJOIN Associativity Rule	Left to Right
   ============================================================
*/
//##ModelId=3B0C086B008F
class EQJOIN_LTOR : public Rule {
 public:
  //##ModelId=3B0C086B00A3
  EQJOIN_LTOR();
  //##ModelId=3B0C086B00A4
  ~EQJOIN_LTOR(){};
  //##ModelId=3B0C086B00AD
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  //##ModelId=3B0C086B00B7
  bool condition(Expression *before, MExression *mexpr, int ContextID);
  //##ModelId=3B0C086B00C3
  int promise(Operator *op_arg, int ContextID) { return ASSOC_PROMISE; };

};  // EQJOIN_LTOR

/*
   ============================================================
   EQJOIN Associativity Rule		Right to Left
   ============================================================
*/
//##ModelId=3B0C086B0161
class EQJOIN_RTOL : public Rule {
 public:
  //##ModelId=3B0C086B016C
  EQJOIN_RTOL();
  //##ModelId=3B0C086B0175
  ~EQJOIN_RTOL(){};
  //##ModelId=3B0C086B0176
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  //##ModelId=3B0C086B0181
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};  // EQJOIN_RTOL

/*
   ============================================================
   Cesar's EXCHANGE Rule: (AxB)x(CxD) -> (AxC)x(BxD)
   ============================================================
*/
class EXCHANGE : public Rule {
 public:
  EXCHANGE();
  ~EXCHANGE(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};

/*
 * Project to Physical Project Rule
 */
//##ModelId=3B0C086B02CA
class P_TO_PP : public Rule {
 public:
  //##ModelId=3B0C086B02DE
  P_TO_PP();
  //##ModelId=3B0C086B02DF
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};  // P_TO_PP

/*
 * Select to filter rule
 */
//##ModelId=3B0C086B0356
class SELECT_TO_FILTER : public Rule {
 public:
  //##ModelId=3B0C086B0361
  SELECT_TO_FILTER();
  //##ModelId=3B0C086B036A
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};  // SELECT_TO_FILTER

//##ModelId=3B0C086C002C
class SORT_RULE : public Rule {
 public:
  //##ModelId=3B0C086C0037
  SORT_RULE();
  //##ModelId=3B0C086C0040
  ~SORT_RULE(){};

  //##ModelId=3B0C086C0041
  int promise(Operator *op_arg, int ContextID);

  //##ModelId=3B0C086C004C
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};  // SORT_RULE

//##ModelId=3B0C086C00D7
class RM_TO_HASH_DUPLICATES : public Rule {
 public:
  RM_TO_HASH_DUPLICATES();
  ~RM_TO_HASH_DUPLICATES(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};

class AL_TO_HGL : public Rule {
 public:
  AL_TO_HGL(AGG_OP_ARRAY *list1, AGG_OP_ARRAY *list2);
  ~AL_TO_HGL(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};

class FO_TO_PFO : public Rule {
 public:
  FO_TO_PFO();
  ~FO_TO_PFO(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};

class AGG_THRU_EQJOIN : public Rule {
 public:
  AGG_THRU_EQJOIN(AGG_OP_ARRAY *list1, AGG_OP_ARRAY *list2);
  ~AGG_THRU_EQJOIN(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};

class EQ_TO_BIT : public Rule {
 public:
  EQ_TO_BIT();
  ~EQ_TO_BIT(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};

class SELECT_TO_INDEXED_FILTER : public Rule {
 public:
  SELECT_TO_INDEXED_FILTER();
  ~SELECT_TO_INDEXED_FILTER(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  bool condition(Expression *before, MExression *mexpr, int ContextID);
};

class PROJECT_THRU_SELECT : public Rule {
 public:
  PROJECT_THRU_SELECT();
  ~PROJECT_THRU_SELECT(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
  // bool condition ( Expression * before, MExression *mexpr, int ContextID);
};

//  DUMMY to PDUMMY Rule
class DUMMY_TO_PDUMMY : public Rule {
 public:
  DUMMY_TO_PDUMMY();
  ~DUMMY_TO_PDUMMY(){};
  Expression *next_substitute(Expression *before, PHYS_PROP *ReqdProp);
};
