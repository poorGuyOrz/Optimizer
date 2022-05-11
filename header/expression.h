#pragma once
#include "op.h"
#include "stdafx.h"

class Expression {
 private:
  Operator *oper;
  vector<Expression *> children_;

 public:
  Expression(Operator *op, vector<Expression *> &&child) : oper(op), children_(child){};

  Expression(Expression &Expr) : oper(Expr.GetOp()->Clone()) {
    for (int i = 0; i < Expr.GetArity(); i++) children_.push_back(new Expression(*(Expr.GetInput(i))));
  };

  ~Expression() {
    delete oper;
    for (auto &&child : children_) delete child;
    children_.clear();
  };

  inline Operator *GetOp() { return oper; };
  inline int GetArity() { return children_.size(); };
  inline Expression *GetInput(int i) { return children_[i]; };

  string Dump() {
    string os;
    os += "(";
    os += (*oper).Dump();
    int i;
    for (auto &&child : children_) {
      os += ",\n";
      ++printnx;
      for (size_t j = 0; j < printnx; j++) os += "    ";
      os += child->Dump();
      --printnx;
    }

    os += ")";
    return os;
  };
};
