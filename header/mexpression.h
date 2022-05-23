#pragma once

#include "expression.h"
#include "op.h"
#include "stdafx.h"

#define NEW_GRPID -1  // used by SearchSpace::CopyIn and MExression::MExressionmeans need to create a new group

class MExression {
 private:
  MExression *HashPtr;  // list within hash bucket
  BIT_VECTOR RuleMask;  // If 1, do not fire rule with that index
  Operator *Op;         // Operator
  vector<int> children_;
  int GrpID;  // I reside in this group

  // link to the next mexpr in the same group
  MExression *NextMExpr;

 public:
  ~MExression() { delete Op; };

  // Transform an Expression into an MExression.  May involve creating new Groups.
  //  GrpID is the ID of the group where the MExression will be put.  If GrpID is
  //  NEW_GRPID(-1), make a new group with that ID.  (Same as SearchSpace::CopyIn)
  MExression(Expression *Expr, int grpid);

  MExression(MExression &other);

  inline Operator *GetOp() { return (Op); };
  inline int GetInput(int i) const { return (children_[i]); };
  inline int GetGrpID() { return (GrpID); };
  inline int GetArity() { return (Op->GetArity()); };

  inline MExression *GetNextHash() { return HashPtr; };
  inline void SetNextHash(MExression *mexpr) { HashPtr = mexpr; };

  inline void SetNextMExpr(MExression *MExpr) { NextMExpr = MExpr; };
  inline MExression *GetNextMExpr() { return NextMExpr; };

  // We just fired this rule, so update dont_fire bit vector
  inline void fire_rule(int rule_no) { bit_on(RuleMask, rule_no); };

  inline void set_rule_mask(BIT_VECTOR v) { RuleMask = v; };

  ub4 hash() {
    ub4 hashval = Op->hash();

    // to check the equality of the inputs
    for (int input_no = Op->GetArity(); --input_no >= 0;) hashval = lookup2(GetInput(input_no), hashval);

    return (hashval % (HtblSize - 1));
  };

  string Dump() {
    string os;

    os = (*Op).Dump();

    int Size = GetArity();
    for (int i = 0; i < Size; i++) os += ", input: " + to_string(children_[i]);

    return os;
  };
};