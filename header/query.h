
#pragma once
#include "../header/item.h"
#include "../header/logop.h"

class Expression;
class Query;

// 编译语句到Expression，SQL的便宜模块，后期可以使用更标准的语法模块替换掉。
// 只要最后的结果是语法树即可
class Query {
 public:
  ~Query();

  // get the Expression from the query tree representation text file
  Query(string filename);

  // return the Expression pointer
  Expression *GetEXPR();

  string Dump();

  // print the interesting orders
  string Dump_IntOrders();

 private:
  Expression *QueryExpr;  // point to the query read in
  string ExprBuf;         // store the original query string

  // get an expression
  Expression *ParseExpr(char *&ExprStr);

  // get one element of the expression
  char *GetOneElement(char *&Expr);

  // get the project keys
  void ParsePJKeys(char *&p, KEYS_SET &Keys);

  // get left and right KEYS_SET
  void ParseKeys(char *p, KEYS_SET &Keys1, KEYS_SET &Keys2);

  // get one KEYS_SET
  void GetOneKeys(char *&p, KEYS_SET &Keys);

  // get one KeySET, and add to KEYS_SET
  void GetKey(char *&p, KEYS_SET &Keys);

  // p points to optional white space followed by a quote mark (").
  // ParseOneParameter returns the string following that ", up to the next ".
  // Upon return, p points to that next quote mark.
  string ParseOneParameter(char *&p);

  // get an AGG_OP_ARRAY
  void ParseAggOps(char *&p, AGG_OP_ARRAY &AggOps);

  // get the group by keys
  void ParseGby(char *&p, KEYS_SET &Keys);

  //	get one AGG_OP
  AGG_OP *GetOneAggOp(char *&p);
};

// An Expression corresponds to a detailed solution to the original query or a subquery.
// An Expression is modeled as an operator with arguments (class Operator), plus input expressions (class Expression).
// EXPRs are used to calculate the initial query and the final plan, and are also used in rules.
// 表达式，可以用来组织SQL，由op和输入expression组成，arity标记输入个数，query初始化的时候使用表达式来表示一个树形语句
class Expression {
 private:
  Operator *Op;         // Operator
  int arity;            // Number of input expressions.
  Expression **Inputs;  // Input expressions

 public:
  Expression(Operator *Op, Expression *First = nullptr, Expression *Second = nullptr, Expression *Third = nullptr,
             Expression *Fourth = nullptr)
      : Op(Op), arity(0) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].New();

    if (First) arity++;
    if (Second) arity++;
    if (Third) arity++;
    if (Fourth) arity++;

    if (arity) {
      Inputs = new Expression *[arity];
      if (First) Inputs[0] = First;
      if (Second) Inputs[1] = Second;
      if (Third) Inputs[2] = Third;
      if (Fourth) Inputs[3] = Fourth;
    }
  };

  Expression(Operator *Op, Expression **inputs) : Op(Op), Inputs(inputs), arity(Op->GetArity()) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].New();
  };

  Expression(Expression &Expr) : Op(Expr.GetOp()->Clone()), arity(Expr.GetArity()) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].New();
    if (arity) {
      Inputs = new Expression *[arity];
      for (int i = 0; i < arity; i++) Inputs[i] = new Expression(*(Expr.GetInput(i)));
    }
  };

  ~Expression() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].Delete();

    delete Op;
    if (arity) {
      for (int i = 0; i < arity; i++) delete Inputs[i];
      delete[] Inputs;
    }
  };

  inline Operator *GetOp() { return Op; };
  inline int GetArity() { return arity; };
  inline Expression *GetInput(int i) { return Inputs[i]; };

  string Dump() {
    string os;
    os += "(";
    os += (*Op).Dump();
    int i;
    for (i = 0; i < arity; i++) {
      os += ",\n";
      ++printnx;
      for (size_t j = 0; j < printnx; j++) os += "    ";
      os += Inputs[i]->Dump();
      --printnx;
    }
    os += ")";
    return os;
  };
};
