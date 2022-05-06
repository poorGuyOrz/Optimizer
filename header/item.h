// item.h - operators on items.  Typically part of a predicate.

#pragma once

#include "op.h"

/* ------------------------------------------------------------ */

/*                         Attribute Operators

  ATTR_OP occupies two roles:

1. It is used in predicates as an item operator, transforming a
tuple into one of its attributes.

2. It is used to describe the logical property "schema".  Here it
refers to two different attribute types:
        a) Attributes read from a catalog, e.g. emp.age, where emp
           is the range variable (default = name of the file), and
           age is a typical attribute
        b) the name for an expression, as in
             SUM(PRICE(1-DISC)) AS revenue
           here "revenue" is the range variable and {PRICE, DISC} is
           the array of attributes (of type 2.a) used in the expression.

Goetz used ATTR_OP for 1 and 2a.
In Columbia, ATTR_OP is 1, ATTR is 2.a, and ATTR_EXP is 2.b .
*/

// ATTR_OP represents the value of an attribute, as in emp.age < 40
class ATTR_OP : public ItemOperator {
 private:
  int AttId;

 public:
  ATTR_OP(int attid) : AttId(attid) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_ATTR_OP].New();
  };

  ATTR_OP(ATTR_OP &Op) : AttId(Op.AttId) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_ATTR_OP].New();
  };

  Operator *Clone() { return new ATTR_OP(*this); };

  ~ATTR_OP() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_ATTR_OP].Delete();
  };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int get_value() { return AttId; }
  inline int GetArity() { return (0); };
  inline string GetName() { return ("ATTR_OP"); };
  inline bool is_const() { return true; };
  inline Cost *get_cost() { return new Cost(0); };

  string Dump() { return "ATTR(" + GetAttName(AttId) + ")"; }
};

class ATTR_EXP : public ItemOperator {
 private:
  string RangeVar;
  int *Atts;
  int AttsSize;
  ATTR *AttNew;

 public:
  ATTR_EXP(string range_var, int *atts, int size);

  ATTR_EXP(ATTR_EXP &Op) : RangeVar(Op.RangeVar), Atts(CopyArray(Op.Atts, Op.AttsSize)), AttsSize(Op.AttsSize) {
    AttNew = new ATTR(*Op.AttNew);
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_ATTR_EXP].New();
  };

  Operator *Clone() { return new ATTR_EXP(*this); };

  ~ATTR_EXP() {
    delete[] Atts;
    delete AttNew;
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_ATTR_EXP].Delete();
  };

  // inline int Get_AttId() { return (AttNew->AttId); };
  inline int GetArity() { return (0); };
  inline int *GetAtts() { return (Atts); };
  inline int GetAttsSize() { return (AttsSize); };
  inline string GetRangeVar() { return (RangeVar); };
  inline ATTR *GetAttNew() { return (AttNew); };
  inline string GetName() { return ("ATTR_EXP"); };

  string Dump();
};

// constant op
class CONST_OP : public ItemOperator {
 public:
  inline bool is_const() { return true; };
  Cost *get_cost() { return new Cost(0); };
};

// Integer valued constant
class CONST_INT_OP : public CONST_OP {
 private:
  int value;

 public:
  // constructors for constant int
  CONST_INT_OP(int value) : value(value) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_INT_OP].New();
  };

  CONST_INT_OP(CONST_INT_OP &Op) : value(Op.value) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_INT_OP].New();
  };

  Operator *Clone() { return new CONST_INT_OP(*this); };

  ~CONST_INT_OP() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_INT_OP].Delete();
  };

  inline int get_value() { return value; }
  inline int GetArity() { return (0); };
  inline string GetName() { return ("INT_OP"); };
  inline bool is_const() { return true; };
  // inline Cost * get_cost() { return new Cost(0); };

  string Dump() { return "INT(%" + to_string(value) + ")"; }
};  // CONST_INT_OP

// String valued constant
class CONST_STR_OP : public CONST_OP {
 private:
  string value;

 public:
  // constructor for constant
  CONST_STR_OP(string value) : value(value) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_STR_OP].New();
  };

  CONST_STR_OP(CONST_STR_OP &Op) : value(Op.value) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_STR_OP].New();
  };

  Operator *Clone() { return new CONST_STR_OP(*this); };

  ~CONST_STR_OP() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_STR_OP].Delete();
  };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline string get_value() { return value; }
  inline int GetArity() { return (0); };
  inline string GetName() { return ("STR_OP"); };
  inline bool is_const() { return true; };
  // inline Cost * get_cost() { return new Cost(0); };

  string Dump() { return "STR(" + value + ")"; }

};  // CONST_STR_OP

// String valued constant SET
class CONST_SET_OP : public CONST_OP {
 private:
  string value;

 public:
  // constructor for constant
  CONST_SET_OP(string value) : value(value) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_SET_OP].New();
  };

  CONST_SET_OP(CONST_SET_OP &Op) : value(Op.value) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_SET_OP].New();
  };

  Operator *Clone() { return new CONST_SET_OP(*this); };

  ~CONST_SET_OP() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_CONST_SET_OP].Delete();
  };

  inline string get_value() { return value; }
  inline int GetArity() { return (0); };
  inline string GetName() { return ("SET_OP"); };
  inline bool is_const() { return true; };
  // inline Cost * get_cost() { return new Cost(0); };

  string Dump() { return "SET(" + value + ")"; };

};  // CONST_SET_OP

/* ------------------------------------------------------------ */

// Boolean Operator
class BOOLE_OP : public ItemOperator {
 private:
 public:
};  // BOOLE_OP

/* ------------------------------------------------------------ */

typedef enum COMP_OP_CODE {
  OP_AND,
  OP_OR,
  OP_NOT,
  OP_EQ,
  OP_LT,
  OP_LE,
  OP_GT,
  OP_GE,
  OP_NE,
  OP_LIKE,
  OP_IN
} COMP_OP_CODE;

// Comparison Operators
class COMP_OP : public BOOLE_OP {
 private:
  COMP_OP_CODE op_code;

 public:
  COMP_OP(COMP_OP_CODE op_code) : op_code(op_code) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_COMP_OP].New();
  };

  COMP_OP(COMP_OP &Op) : op_code(Op.op_code) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_COMP_OP].New();
  };

  Operator *Clone() { return new COMP_OP(*this); };

  ~COMP_OP() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_COMP_OP].Delete();
  };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() {
    if (op_code == OP_NOT)
      return (1);
    else
      return (2);
  };

  inline string GetName() { return ("COMP_OP"); };

  string Dump() {
    string os;
    switch (op_code) {
      case OP_AND:
        os = "OP_AND";
        break;
      case OP_OR:
        os = "OP_OR";
        break;
      case OP_NOT:
        os = "OP_NOT";
        break;
      case OP_EQ:
        os = "OP_EQ";
        break;
      case OP_LT:
        os = "OP_LT";
        break;
      case OP_LE:
        os = "OP_LE";
        break;
      case OP_GT:
        os = "OP_GT";
        break;
      case OP_GE:
        os = "OP_GE";
        break;
      case OP_NE:
        os = "OP_NE";
        break;
      case OP_LIKE:
        os = "OP_LIKE";
        break;
      case OP_IN:
        os = "OP_IN";
        break;

      default:
        assert(false);
    }
    return os;
  }
};
