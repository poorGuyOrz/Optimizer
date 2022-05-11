
#pragma once
#include "../header/item.h"
#include "../header/logop.h"

class Expression;
class Query;

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
