#pragma once
#include "op.h"

#define GET_ID 1111  // log_op name id for hash or compare
#define EQJOIN_ID 2222
#define PROJECT_ID 3333
#define SELECT_ID 4444
#define RM_DUPLICATES_ID 5555
#define AGG_LIST_ID 6666
#define FUNC_OP_ID 7777
#define DUMMY_ID 8888

class GET;
class SELECT;
class PROJECT;
class EQJOIN;
class DUMMY;
class RM_DUPLICATES;
class AGG_LIST;
class FUNC_OP;

/*
============================================================
Class GET - GET DATA FROM DISK
============================================================
The GET operator retrieves data from disk storage.  It has no inputs
since it is the leaf operator in logical expressions.  The data retrieved
is specified by the CollID and RangeVar.  If the query included
from EMP as E, then EMP (actually, its ID) is the CollID and
E is the RangeVar.
*/

class GET : public LogicalOperator {
 public:
  // If the query includes FROM EMP e, then EMP is the collection
  // and e is the range variable.
  GET(string collection, string rangeVar);
  GET(int collId);  // Range Variable defaults to Collection here
  GET(GET &Op);
  Operator *Clone() { return new GET(*this); };

  ~GET(){};

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (0); };
  inline string GetName() { return ("GET"); };
  inline int GetNameId() { return GET_ID; };
  inline int GetCollection() { return CollId; };
  inline bool operator==(Operator *other) {
    return (other->GetNameId() == GetNameId() && ((GET *)other)->CollId == CollId &&
            ((GET *)other)->RangeVar == RangeVar);
  };

  string Dump();

  // since this operator has arguments
  ub4 hash();

 private:
  int CollId;
  string RangeVar;
};

/*
   ============================================================
   EQJOIN
   ============================================================
   Natural Join, or Equijoin.  Since the predicates are so simple they
   are modeled as arguments.  If we ever get to manipulating general predicates,
   we may want to reconsider this decision.
*/

class EQJOIN : public LogicalOperator {
 public:
  // If the query includes
  //  	WHERE A.X = B.Y AND C.Z = D.W
  // then
  // lattrs is <A.X, C.Z> and
  //  rattrs is <B.Y, D.W>
  int *lattrs;  // left attr's that are the same
                //##ModelId=3B0C087302FD
  int *rattrs;  // right attr's that are the same
  int size;     // number of the attrs

 public:
  EQJOIN(int *lattrs, int *rattrs, int size);
  EQJOIN(EQJOIN &Op);
  Operator *Clone() { return new EQJOIN(*this); };

  ~EQJOIN() {
    delete[] lattrs;
    delete[] rattrs;
  };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (2); };           // Inputs are left and right streams
  inline string GetName() { return ("EQJOIN"); };  // Name of this operator
  inline int GetNameId() { return EQJOIN_ID; };    // Name of this operator
  inline bool operator==(Operator *other) {
    return (other->GetNameId() == GetNameId() &&
            EqualArray(((EQJOIN *)other)->lattrs, lattrs, size) &&  // arguments are equal
            EqualArray(((EQJOIN *)other)->rattrs, rattrs, size));
  };

  // since this operator has arguments
  ub4 hash();

  string Dump();

};  // EQJOIN

/*
============================================================
DUMMY
============================================================
For Paul Benninghoff's experiment.  Used to determine if optimizing two queries at
once can result in significant savings.  DUMMY has two inputs, no transforms, one
implementation, namely PDUMMY.
*/

class DUMMY : public LogicalOperator {
 public:
  DUMMY();
  DUMMY(DUMMY &Op);
  Operator *Clone() { return new DUMMY(*this); };

  ~DUMMY(){};

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (2); };          // Inputs are left and right streams
  inline string GetName() { return ("DUMMY"); };  // Name of this operator
  inline int GetNameId() { return DUMMY_ID; };    // Name of this operator
  inline bool operator==(Operator *other) { return (other->GetNameId() == GetNameId()); };

  ub4 hash();

  string Dump();

};  // DUMMY

/* ============================================================
   SELECT
   ============================================================
   This is the standard select operator: only input objects satisfying a predicate
   will be output.  However, we model the predicate of the operator as its second
   input.  Cascades modeled predicates as inputs in general, so that their structure
   could be handled with the Cascades transformation mechanism.
*/

class SELECT : public LogicalOperator {
 public:
  SELECT();
  SELECT(SELECT &Op);
  Operator *Clone() { return new SELECT(*this); };

  ~SELECT(){};

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (2); };           // For input and predicate
  inline string GetName() { return ("SELECT"); };  // Name of this operator
  inline int GetNameId() { return SELECT_ID; };    // Name of this operator
  inline bool operator==(Operator *other) { return (other->GetNameId() == GetNameId()); }

  ub4 hash();

  string Dump();
};

/*
   ============================================================
   PROJECT
   ============================================================
   The standard project operator.  Attributes to project on are
   modeled as arguments.
*/

class PROJECT : public LogicalOperator {
 public:
  int *attrs;  // attr's to project on
  int size;    // the number of the attrs

  PROJECT(int *attrs, int size);
  PROJECT(PROJECT &Op);
  Operator *Clone() { return new PROJECT(*this); };

  ~PROJECT() { delete[] attrs; };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (1); };  // Only input is the stream of bytes.  What to project is an argument, pattrs
  inline string GetName() { return ("PROJECT"); };  // Name of this operator
  inline int GetNameId() { return PROJECT_ID; };    // Name of this operator
  inline bool operator==(Operator *other) {
    return (other->GetNameId() == GetNameId() &&
            EqualArray(((PROJECT *)other)->attrs, attrs, size));  // arguments are equal
  };

  // since this operator has arguments
  ub4 hash();

  string Dump();

};  // PROJECT

/*
   ============================================================
   RM_DUPLICATES
   ============================================================
   standard remove duplicats operator.  The only input is the collections
   on which duplicates are removed
*/

class RM_DUPLICATES : public LogicalOperator {
 public:
  RM_DUPLICATES();
  RM_DUPLICATES(RM_DUPLICATES &Op);
  Operator *Clone() { return new RM_DUPLICATES(*this); };

  ~RM_DUPLICATES(){};

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (1); };                  // Only input is the stream of bytes.
  inline string GetName() { return ("RM_DUPLICATES"); };  // Name of this operator
  inline int GetNameId() { return RM_DUPLICATES_ID; };    // Name of this operator
  inline bool operator==(Operator *other) { return (other->GetNameId() == GetNameId()); };

  // since this operator has arguments
  ub4 hash();

  string Dump();
};

/*
   ============================================================
   AGG_LIST
   ============================================================
   This operator combines aggregate functions (via AggOps),
        and the list of attributes grouped by (via GbyAtts).
        For example,
                SELECT SUM(LE*(1-LD)) AS revenue
                FROM LineItem
                Group BY LD, LS
        becomes the operator
                AGG_LIST with parameters
                        AggOps = <LE, LD> and GbyAtts <LE, LD>
        Note that AggOps and GbyAtts are arrays in general.  In this
        example each array has one element.
*/

class AGG_LIST : public LogicalOperator {
 public:
  int *GbyAtts;
  int GbySize;
  AGG_OP_ARRAY *AggOps;
  int *FlattenedAtts;
  int FAttsSize;

  AGG_LIST(int *gby_atts, int gby_size, AGG_OP_ARRAY *agg_ops);
  AGG_LIST(AGG_LIST &Op)
      : GbyAtts(CopyArray(Op.GbyAtts, Op.GbySize)),
        GbySize(Op.GbySize),
        FAttsSize(Op.FAttsSize),
        FlattenedAtts(CopyArray(Op.FlattenedAtts, Op.FAttsSize)) {
    AggOps = new AGG_OP_ARRAY;
    AggOps->resize(Op.AggOps->size());
    for (int i = 0; i < Op.AggOps->size(); i++) {
      (*AggOps)[i] = new AGG_OP(*(*Op.AggOps)[i]);
    }
  };

  Operator *Clone() { return new AGG_LIST(*this); };

  ~AGG_LIST() {
    delete[] GbyAtts;
    for (int i = 0; i < AggOps->size(); i++) delete (*AggOps)[i];
    delete AggOps;
    delete[] FlattenedAtts;
  };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (1); };             // Only input is the stream of bytes.
  inline string GetName() { return ("AGG_LIST"); };  // Name of this operator
  inline int GetNameId() { return AGG_LIST_ID; };    // Name of this operator
  bool operator==(Operator *other);

  // since this operator has arguments
  ub4 hash();

  string Dump();
};

/*
   ============================================================
   FUNC_OP
   ============================================================
   FUNC_OP represents an operator which applies a function to the
   attributes in "attrs".  The function could be just arithmetic;
   if it involves an aggregate, see AGG_OP
*/

class FUNC_OP : public LogicalOperator {
 public:
  string RangeVar;
  int *Atts;
  int AttsSize;

  FUNC_OP(string range_var, int *atts, int size) : RangeVar(range_var), Atts(atts), AttsSize(size){};

  FUNC_OP(FUNC_OP &Op) : RangeVar(Op.RangeVar), Atts(CopyArray(Op.Atts, Op.AttsSize)), AttsSize(Op.AttsSize){};

  Operator *Clone() { return new FUNC_OP(*this); };

  ~FUNC_OP() { delete[] Atts; };

  LOG_PROP *FindLogProp(LOG_PROP **input);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("FUNC_OP"); };
  inline int GetNameId() { return FUNC_OP_ID; };
  inline bool operator==(Operator *other) {
    return (other->GetNameId() == GetNameId() &&
            EqualArray(((FUNC_OP *)other)->Atts, Atts, AttsSize) &&  // arguments are equal
            ((FUNC_OP *)other)->RangeVar == RangeVar);
  };

  // since this operator has arguments
  ub4 hash();

  string Dump();
};
