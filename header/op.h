// Operator.H - Base Classes for Operators

#pragma once
#include "../header/stdafx.h"

class Operator;  // All operators - Abstract

class LogicalOperator;   // Logical Operators on Collections
class PhysicalOperator;  // Physical Operators on Collections
class ItemOperator;      // Item Operators on objects, used for predicates

class LeafOperator;  // Leaf operators - place holder for a group, in a pattern. Patterns are used in rules.

// Abstract Class.  Operator and its arguments.
// Arguments could be attributes to project on, etc.
class Operator {
 public:
  string name;  // Name of this operator

  Operator(){};

  // add assert to the following virtual functions, make sure the subclasses define them
  virtual Operator *Clone() = 0;

  virtual ~Operator(){};

  virtual string Dump() = 0;
  virtual LOG_PROP *FindLogProp(LOG_PROP **input) = 0;

  // For example, the operator SELECT has arity 2 (one bulk input, one predicate) and
  // its name is SELECT.  These are not stored as member data since static does not
  // inherit and we don't want one in every object.
  virtual string GetName() = 0;
  virtual int GetNameId() = 0;
  virtual int GetArity() = 0;

  virtual bool operator==(Operator *other) { return (GetNameId() == other->GetNameId()); };

  // Used to compute the hash value of an mexpr.  Used only for logical operators,
  // so we make it abort everywhere else.
  virtual ub4 hash() = 0;

  virtual bool is_logical() { return false; };
  virtual bool is_physical() { return false; };
  virtual bool is_leaf() { return false; };
  virtual bool is_item() { return false; };

  // attr_op, const_int_op, const_set_op, const_str_op are special case in O_INPUT::perform()
  virtual bool is_const() { return false; };

  // 计算表达式的代价
  // 逻辑表达式没有代价，物理表达式才计算代价
  virtual Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) = 0;
};

// Logical Operator Abstract Class
class LogicalOperator : public Operator {
 public:
  LogicalOperator(){};
  virtual ~LogicalOperator(){};

  // OpMatch (other) is true if this and other are the same operator,
  // independent of arguments.
  // OpMatch is used in preconditions for applying rules.
  // This should be moved to the Operator class if we ever apply rules to
  // other than logical operators.
  // If someone writes a rule which uses member data, it could be made virtual
  inline bool OpMatch(LogicalOperator *other) { return (GetNameId() == other->GetNameId()); };

  inline bool is_logical() { return true; };

  inline ub4 GetInitval() { return (lookup2(GetNameId(), 0)); };
  // Get the initial value for hashing, which depends
  // only on the name of the operator.

  // add assert to the following functions,
  // make sure these methods of LogicalOperator never called(log_op does not get cost)
  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
    assert(false);
    return nullptr;
  };
};

// Physical Operator
class PhysicalOperator : public Operator {
 public:
  PhysicalOperator(){};
  virtual ~PhysicalOperator(){};

  // FindPhysProp() establishes the physical properties of an algorithm's output.
  //  right now, only implemented by operators with 0 arity. no input_phys_props
  virtual PHYS_PROP *FindPhysProp(PHYS_PROP **input_phys_props = nullptr) {
    assert(false);
    return nullptr;
  };

  // FindLocalCost() finds the local cost of the operator,
  // including output but not input costs.  Thus we compute output costs
  // only once, and get input costs from (as part of) the input operators' cost.
  virtual Cost *FindLocalCost(LOG_PROP *LocalLogProp,        // uses primarily the card of the Group
                              LOG_PROP **InputLogProp) = 0;  // uses primarily cardinalities

  /*  Some algorithms and implementation rules require that
their inputs be optimized for multiple physical property
combinations, e.g., a merge-join with multiple equality clauses
"R.a == S.a && R.b == S.b" could benefit from sort order on
"a", "b", "a, b", and "b, a".  For now we optimize for only
  one ordering, but later we may need:
virtual int opt_combs() */

  // If we require the physical property Prop of this operator, what
  // property from input number InputNo will guarantee it?
  // A false return value for possible means there is no value which will work.
  // If possible is true, a nullptr return says any property is OK.
  // Should never be called for arity 0 operators
  virtual PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) = 0;

  inline bool is_physical() { return true; };
  // add assert to the following functions,
  // make sure these methods of PhysicalOperator never called(log_prop is not passed by phys_op)
  LOG_PROP *FindLogProp(LOG_PROP **input) {
    assert(false);
    return nullptr;
  };
  inline int GetNameId() {
    assert(false);
    return 0;
  };
  ub4 hash() {
    assert(false);
    return 0;
  };
};  // class PhysicalOperator

/*
    ============================================================
        ITEM OPERATORS - ACT ON INDIVIDUAL OBJECTS - USED IN PREDICATES - class ItemOperator
    ============================================================
*/

// Item Operator - both logical and physical
// Can we do multiple inheritance?  That would be ideal.
class ItemOperator : public Operator {
 public:
  ItemOperator(){};
  ~ItemOperator(){};

  // For now we assume no expensive predicates
  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) { return (new Cost(0)); };

  LOG_PROP *FindLogProp(LOG_PROP **input) {
    KEYS_SET empty_arr;
    return (new LOG_ITEM_PROP(-1, -1, -1, 0, empty_arr));
  }

  inline bool is_item() { return true; };

  // add assert to the following functions,
  // make sure these methods of ItemOperator never called
  inline int GetNameId() {
    assert(false);
    return 0;
  };
  ub4 hash() {
    assert(false);
    return 0;
  };

};  // class ItemOperator

// Used in rules only.  Placeholder for a Group
class LeafOperator : public Operator {
 private:
  int Group;  // Identifies the group bound to this leaf, after binding.
              //  == -1 until binding
  int Index;  // Used to distinguish this leaf in a rule

 public:
  LeafOperator(int index, int group = -1) : Index(index), Group(group) {
    name = GetName();  // for debug
  };

  LeafOperator(LeafOperator &Op) : Index(Op.Index), Group(Op.Group) {
    name = Op.name;  // for debug
  };

  inline Operator *Clone() { return new LeafOperator(*this); };

  ~LeafOperator(){};

  inline int GetArity() { return (0); };
  inline string GetName() { return ("LeafOperator"); };
  inline int GetGroup() { return (Group); };
  inline int GetIndex() { return (Index); };

  inline bool is_leaf() { return true; };

  string Dump() { return GetName() + "<" + to_string(Index) + "," + to_string(Group) + ">"; };

  // add assert to the following functions,
  // make sure these methods of LeafOperator never called
  inline int GetNameId() {
    assert(false);
    return 0;
  };
  ub4 hash() {
    assert(false);
    return 0;
  };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
    assert(false);
    return nullptr;
  };

  LOG_PROP *FindLogProp(LOG_PROP **input) {
    assert(false);
    return nullptr;
  };

};  // class LeafOperator

/*
    ============================================================
    AGG_OP - USED IN Aggregates - Not an Operator
    ============================================================
*/

// AGG_OP represents an operator which applies an aggregate function to the
// attributes in "attrs".  The function could be SUM COUNT MIN MAX etc.;
// AGG_OP may also involve a more complex expression, e.g., with more than one function.
// It is part of the logical operator AGG_LIST and physical op HGROUP_LIST
class AGG_OP {
 private:
  string RangeVar;  // Name assigned to the function, e.g. REVENUE = SUM(xxx)
  int *Atts;        // Attributes involved in the function
  int AttsSize;     // Number of attributes above
 public:
  AGG_OP(string range_var, int *atts, int size) : RangeVar(range_var), Atts(atts), AttsSize(size){};

  AGG_OP(AGG_OP &Op) : RangeVar(Op.RangeVar), AttsSize(Op.AttsSize), Atts(CopyArray(Op.Atts, Op.AttsSize)){};

  ~AGG_OP() { delete[] Atts; };

  inline string GetName() { return ("AGG_OP"); };
  inline string GetRangeVar() { return (RangeVar); };
  inline int *GetAtts() { return (Atts); };
  inline int GetAttsSize() { return (AttsSize); };

  string Dump() {
    string os;
    os = GetName() + " <";
    for (int i = 0; i < AttsSize - 1; i++) os += GetAttName(Atts[i]) + ",";

    if (AttsSize == 0)
      os += "Empty set";
    else
      os += GetAttName(Atts[AttsSize - 1]);

    os += " AS " + RangeVar + " >";
    return os;
  };

  bool operator==(AGG_OP *other) {
    return (other->GetName() == this->GetName() && EqualArray(other->GetAtts(), Atts, AttsSize) &&
            other->GetRangeVar() == RangeVar);
  };
};

typedef vector<AGG_OP *> AGG_OP_ARRAY;
