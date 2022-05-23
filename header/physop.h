#pragma once
#include "op.h"

class FILE_SCAN;
class LOOPS_JOIN;
class LOOPS_INDEX_JOIN;
class MERGE_JOIN;
class P_PROJECT;
class FILTER;
class INDEXED_FILTER;
class QSORT;
class HASH_DUPLICATES;
class HGROUP_LIST;
class P_FUNC_OP;
class BIT_JOIN;

// Physical version of GET.  Retrieves all data from the specified file.
class FILE_SCAN : public PhysicalOperator {
 public:
  FILE_SCAN(int fileId);
  FILE_SCAN(FILE_SCAN &Op);

  inline Operator *Clone() { return new FILE_SCAN(*this); };

  ~FILE_SCAN(){};

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp);

  PHYS_PROP *FindPhysProp(PHYS_PROP **input_phys_props = nullptr);

  inline string GetName() { return ("FILE_SCAN"); };
  inline int GetArity() { return (0); };
  inline int GetFileId() { return FileId; };

  string Dump() { return GetName() + "(" + GetCollName(FileId) + ")"; };

  // File_scan does not have any InputReqProp
  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
    assert(false);
    return nullptr;
  };

 private:
  int FileId;
  string RangeVar;
};

//  A physical version of EQJOIN.  Nested loops, not index nested loops.
class LOOPS_JOIN : public PhysicalOperator {
 public:
  int *lattrs;
  int *rattrs;
  int size;

 public:
  LOOPS_JOIN(int *lattrs, int *rattrs, int size);
  LOOPS_JOIN(LOOPS_JOIN &Op);

  inline Operator *Clone() { return new LOOPS_JOIN(*this); };

  ~LOOPS_JOIN() {
    delete[] lattrs;
    delete[] rattrs;
  };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp);

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);
  inline int GetArity() { return (2); };
  inline string GetName() { return ("LOOPS_JOIN"); };

  string Dump();
};

// Physical Dummy Operator.  Just to give the dummy operator a physical counterpart.
class PDUMMY : public PhysicalOperator {
 public:
  PDUMMY();
  PDUMMY(PDUMMY &Op);

  inline Operator *Clone() { return new PDUMMY(*this); };

  ~PDUMMY(){};

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp);

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);
  inline int GetArity() { return (2); };
  inline string GetName() { return ("PDUMMY"); };

  string Dump();
};

/*
   Nested loops index join
   =======================
*/

class LOOPS_INDEX_JOIN : public PhysicalOperator {
 public:
  int *lattrs;  // left attr's that are the same
  int *rattrs;  // right attr's that are the same
  int size;     // the number of the attrs
  int CollId;   // collection id accessed thru index

 public:
  LOOPS_INDEX_JOIN(int *lattrs, int *rattrs, int size, int CollId);
  LOOPS_INDEX_JOIN(LOOPS_INDEX_JOIN &Op);

  inline Operator *Clone() { return new LOOPS_INDEX_JOIN(*this); };

  ~LOOPS_INDEX_JOIN() {
    delete[] lattrs;
    delete[] rattrs;
  };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("LOOPS_INDEX_JOIN"); };

  string Dump();
};  // LOOPS_INDEX_JOIN

/*
   Merge join
   =================
   A Merge eqjoin
*/

//##ModelId=3B0C086F010D
class MERGE_JOIN : public PhysicalOperator {
 public:
  //##ModelId=3B0C086F0118
  int *lattrs;  // left attr's that are the same
  //##ModelId=3B0C086F012B
  int *rattrs;  // right attr's that are the same
  //##ModelId=3B0C086F0135
  int size;  // the number of the attrs

 public:
  //##ModelId=3B0C086F013F
  MERGE_JOIN(int *lattrs, int *rattrs, int size);

  //##ModelId=3B0C086F014B
  MERGE_JOIN(MERGE_JOIN &Op);

  //##ModelId=3B0C086F0154
  inline Operator *Clone() { return new MERGE_JOIN(*this); };

  //##ModelId=3B0C086F015D
  ~MERGE_JOIN() {
    delete[] lattrs;
    delete[] rattrs;
  };

  //##ModelId=3B0C086F0167
  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  //##ModelId=3B0C086F0171
  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  //##ModelId=3B0C086F0185
  inline int GetArity() { return (2); };
  //##ModelId=3B0C086F0186
  inline string GetName() { return ("MERGE_JOIN"); };
  //##ModelId=3B0C086F018F
  PHYS_PROP *FindPhysProp(PHYS_PROP **input_phys_props);
  //##ModelId=3B0C086F0199
  string Dump();
};  // MERGE_JOIN

// Does not require its inputs to be hashed
//##ModelId=3B0C086F0225
class HASH_JOIN : public PhysicalOperator {
 public:
  int *lattrs, *rattrs;  // left, right attr's to be joined
  //##ModelId=3B0C086F0230
  int size;  // the number of attrs

  //##ModelId=3B0C086F0239
  HASH_JOIN(int *lattrs, int *rattrs, int size);
  //##ModelId=3B0C086F024D
  HASH_JOIN(HASH_JOIN &Op);

  //##ModelId=3B0C086F024F
  inline Operator *Clone() { return new HASH_JOIN(*this); };

  //##ModelId=3B0C086F0257
  ~HASH_JOIN() {
    delete[] lattrs;
    delete[] rattrs;
  };

  //##ModelId=3B0C086F0261
  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  //##ModelId=3B0C086F026B
  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  //##ModelId=3B0C086F0278
  inline int GetArity() { return (2); };
  //##ModelId=3B0C086F0280
  inline string GetName() { return ("HASH_JOIN"); };
  //##ModelId=3B0C086F028A
  string Dump();

};  // HASH_JOIN
//##ModelId=3B0C086F0320
class P_PROJECT : public PhysicalOperator {
 public:
  //##ModelId=3B0C086F0334
  int *attrs;  // attr's to project on
  //##ModelId=3B0C086F033E
  int size;

 public:
  //##ModelId=3B0C086F0348
  P_PROJECT(int *attrs, int size);
  //##ModelId=3B0C086F0352
  P_PROJECT(P_PROJECT &Op);

  //##ModelId=3B0C086F035C
  inline Operator *Clone() { return new P_PROJECT(*this); };

  //##ModelId=3B0C086F0366
  ~P_PROJECT() { delete[] attrs; };

  //##ModelId=3B0C086F0367
  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  //##ModelId=3B0C086F0372
  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  //##ModelId=3B0C086F0384
  inline int GetArity() { return (1); };
  //##ModelId=3B0C086F038E
  inline string GetName() { return ("P_PROJECT"); };

  //##ModelId=3B0C086F038F
  string Dump();

};  // P_PROJECT

/*
   Filter
   ======
*/

//##ModelId=3B0C08700028
class FILTER : public PhysicalOperator {
 public:
  //##ModelId=3B0C08700033
  FILTER(){};
  //##ModelId=3B0C0870003C
  FILTER(FILTER &Op){};
  //##ModelId=3B0C0870003E
  ~FILTER(){};

  //##ModelId=3B0C08700046
  inline Operator *Clone() { return new FILTER(*this); };

  //##ModelId=3B0C08700050
  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  //##ModelId=3B0C0870005C
  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  //##ModelId=3B0C0870006E
  inline int GetArity() { return (2); };
  //##ModelId=3B0C08700078
  inline string GetName() { return ("FILTER"); };

  //##ModelId=3B0C08700079
  string Dump() { return GetName(); };

};  // FILTER

/*
   Sorting
   =======
*/

class QSORT : public PhysicalOperator {
 public:
  QSORT();

  QSORT(QSORT &Op);

  inline Operator *Clone() { return new QSORT(*this); };

  ~QSORT(){};

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp);

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("QSORT"); };

  string Dump();
};  // QSORT

class HASH_DUPLICATES : public PhysicalOperator {
 public:
  HASH_DUPLICATES(){};
  HASH_DUPLICATES(HASH_DUPLICATES &Op){};

  inline Operator *Clone() { return new HASH_DUPLICATES(*this); };

  ~HASH_DUPLICATES(){};

  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("HASH_DUPLICATES"); };

  string Dump();

};  // HASH_DUPLICATES

// Hash-based AGG_LIST
class HGROUP_LIST : public PhysicalOperator {
 public:
  AGG_OP_ARRAY *AggOps;
  int *GbyAtts;
  int GbySize;

 public:
  HGROUP_LIST(int *gby_atts, int gby_size, AGG_OP_ARRAY *agg_ops)
      : GbyAtts(gby_atts), GbySize(gby_size), AggOps(agg_ops){};

  HGROUP_LIST(HGROUP_LIST &Op) : GbyAtts(CopyArray(Op.GbyAtts, Op.GbySize)), GbySize(Op.GbySize) {
    AggOps = new AGG_OP_ARRAY;
    AggOps->resize(Op.AggOps->size());
    for (int i = 0; i < Op.AggOps->size(); i++) {
      (*AggOps)[i] = new AGG_OP(*(*Op.AggOps)[i]);
    }
  };

  inline Operator *Clone() { return new HGROUP_LIST(*this); };

  ~HGROUP_LIST() {
    for (int i = 0; i < AggOps->size(); i++) delete (*AggOps)[i];
    delete AggOps;
    delete[] GbyAtts;
  };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("HGROUP_LIST"); };
  PHYS_PROP *FindPhysProp(PHYS_PROP **input_phys_props);
  string Dump();

};  // HASH_DUPLICATES

class P_FUNC_OP : public PhysicalOperator {
 public:
  string RangeVar;
  int *Atts;
  int AttsSize;

  P_FUNC_OP(string range_var, int *atts, int size) : RangeVar(range_var), Atts(atts), AttsSize(size){};

  P_FUNC_OP(P_FUNC_OP &Op) : RangeVar(Op.RangeVar), Atts(CopyArray(Op.Atts, Op.AttsSize)), AttsSize(Op.AttsSize){};

  inline Operator *Clone() { return new P_FUNC_OP(*this); };

  ~P_FUNC_OP() { delete[] Atts; };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp);

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("P_FUNC_OP"); };

  string Dump();
};

/*
   Bit index join    This is a semi-join
   =======================
*/

class BIT_JOIN : public PhysicalOperator {
 public:
  int *lattrs;  // left attr's that are the same
  int *rattrs;  // right attr's that are the same
  int size;     // the number of the attrs
  int CollId;   // collection id accessed thru BIT index

 public:
  BIT_JOIN(int *lattrs, int *rattrs, int size, int CollId);
  BIT_JOIN(BIT_JOIN &Op);

  inline Operator *Clone() { return new BIT_JOIN(*this); };

  ~BIT_JOIN() {
    delete[] lattrs;
    delete[] rattrs;
  };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp,    // uses primarily the card of the Group
                      LOG_PROP **InputLogProp);  // uses primarily cardinalities

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);
  PHYS_PROP *FindPhysProp(PHYS_PROP **input_phys_props);

  inline int GetArity() { return (2); };
  inline string GetName() { return ("BIT_JOIN"); };

  string Dump();
};

//  Indexed_Filter
class INDEXED_FILTER : public PhysicalOperator {
 public:
  INDEXED_FILTER(const int fileId);
  INDEXED_FILTER(INDEXED_FILTER &Op);
  ~INDEXED_FILTER(){};

  inline Operator *Clone() { return new INDEXED_FILTER(*this); };

  Cost *FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp);

  PHYS_PROP *InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible);

  inline int GetArity() { return (1); };
  inline string GetName() { return ("INDEXED_FILTER"); };
  inline int GetFileId() { return FileId; };

  string Dump() { return GetName() + "(" + GetCollName(FileId) + ")"; };

 private:
  int FileId;
  string RangeVar;
};
