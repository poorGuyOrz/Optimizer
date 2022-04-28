/* query.h - Definition of query parser
$Revision: 3 $
Columbia Optimizer Framework

  A Joint Research Project of Portland State University
  and the Oregon Graduate Institute
  Directed by Leonard Shapiro and David Maier
  Supported by NSF Grants IRI-9610013 and IRI-9619977


*/

#ifndef QUERY_H
#define QUERY_H

#include "../header/item.h"
#include "../header/logop.h"

class EXPR;
class QUERY;

/*
============================================================
QUERY Parser - class QUERY
============================================================
*/

class QUERY {
 public:
  // free up memory
  ~QUERY();

  // get the EXPR from the query tree representation text file
  QUERY(string filename);

  // return the EXPR pointer
  EXPR *GetEXPR();

  string Dump();

  // print the interesting orders
  string Dump_IntOrders();

 private:
  EXPR *QueryExpr;  // point to the query read in
  string ExprBuf;   // store the original query string

  // get an expression
  EXPR *ParseExpr(char *&ExprStr);

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

/*
============================================================
EXPRESSIONS - class EXPR
============================================================
*/

class EXPR  // An EXPR corresponds to a detailed solution to
// the original query or a subquery.
// An EXPR is modeled as an operator with arguments (class OP),
// plus input expressions (class EXPR).

// EXPRs are used to calculate the initial query and the final plan,
// and are also used in rules.

{
 private:
  OP *Op;         // Operator
  int arity;      // Number of input expressions.
  EXPR **Inputs;  // Input expressions
 public:
  EXPR(OP *Op, EXPR *First = NULL, EXPR *Second = NULL, EXPR *Third = NULL, EXPR *Fourth = NULL) : Op(Op), arity(0) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].New();

    if (First) arity++;
    if (Second) arity++;
    if (Third) arity++;
    if (Fourth) arity++;

    if (arity) {
      Inputs = new EXPR *[arity];
      if (First) Inputs[0] = First;
      if (Second) Inputs[1] = Second;
      if (Third) Inputs[2] = Third;
      if (Fourth) Inputs[3] = Fourth;
    }
  };

  EXPR(OP *Op, EXPR **inputs) : Op(Op), Inputs(inputs), arity(Op->GetArity()) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].New();
  };

  EXPR(EXPR &Expr) : Op(Expr.GetOp()->Clone()), arity(Expr.GetArity()) {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].New();
    if (arity) {
      Inputs = new EXPR *[arity];
      for (int i = 0; i < arity; i++) Inputs[i] = new EXPR(*(Expr.GetInput(i)));
    }
  };

  ~EXPR() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_EXPR].Delete();

    delete Op;
    Op = NULL;
    if (arity) {
      for (int i = 0; i < arity; i++) delete Inputs[i];
      delete[] Inputs;
    }
  };

  inline OP *GetOp() { return Op; };
  inline int GetArity() { return arity; };
  inline EXPR *GetInput(int i) { return Inputs[i]; };

  string Dump() {
    string os;
    os += "(\n";
    os += (*Op).Dump();
    int i;
    for (i = 0; i < arity; i++) {
      os += ",\n";
      os += Inputs[i]->Dump();
    }
    os += "\n)";
    return os;
  };


};  // class  EXPR

#endif  // QUERY_H
