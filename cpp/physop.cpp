#include "../header/physop.h"

#include "../header/cat.h"
#include "../header/cm.h"
#include "../header/stdafx.h"

/* need for group pruning, calculate the TouchCopy cost of the expr
TouchCopyCost =
TouchCopy() * |G| +     //From top join
TouchCopy() * sum(cucard(Ai) i = 2, ..., n-1) +  // from other, nontop,
*/                                                        // joins

double TouchCopyCost(LOG_COLL_PROP *LogProp) {
  double Total;

  if (LogProp->Card == -1)
    Total = 0;  // Card is unknown
  else
    Total = LogProp->Card;  // from top join

  // from A2 -- An-1 , means excluding the min and max cucard(i)
  double Min = 3.4E+38;
  double Max = 0;
  for (int i = 0; i < LogProp->Schema->GetTableNum(); i++) {
    float CuCard = LogProp->Schema->GetTableMaxCuCard(i);
    if (Min > CuCard) Min = CuCard;
    if (Max < CuCard) Max = CuCard;
    Total += CuCard;
  }

  // exclude min and max
  Total -= Min;
  Total -= Max;

  return (Total * costModel->touch_copy());
};

/* for cucard pruning, calculate the minimun cost of fetching cucard tuples from disc
        FetchingCost =
        Fetch() * sum(cucard(Ai) i = 1, ..., n) // from leaf fetches
        For each Ai which has no index on a join (interesting) order, replace
        cucard(Ai) with |Ai| and it is still a lower bound.
*/
double FetchingCost(LOG_COLL_PROP *LogProp) {
  double Total = 0;

  for (int i = 0; i < LogProp->Schema->GetTableNum(); i++) {
    float CuCard = LogProp->Schema->GetTableMaxCuCard(i);
    float Width = LogProp->Schema->GetTableWidth(i);
    Total += ceil(CuCard * Width) * (costModel->cpu_read() +  // cpu cost of reading from disk
                                     costModel->io());        // i/o cost of reading from disk
  }

  return Total;
}

FILE_SCAN ::FILE_SCAN(const int fileId) : FileId(fileId) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_FILE_SCAN].New();
  name = GetName() + GetCollName(FileId);
};

FILE_SCAN::FILE_SCAN(FILE_SCAN &Op) : FileId(Op.GetFileId()) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_FILE_SCAN].New();
  name = Op.name;
}

PHYS_PROP *FILE_SCAN::FindPhysProp(PHYS_PROP **input_phys_props) {
  COLL_PROP *CollProp = Cat->GetCollProp(FileId);

  if (CollProp->Order == any) {
    return new PHYS_PROP(any);
  } else {
    PHYS_PROP *phys_prop = new PHYS_PROP(new KEYS_SET(*(CollProp->Keys)), CollProp->Order);
    if (CollProp->Order == sorted)
      for (int i = 0; i < CollProp->KeyOrder.size(); i++) phys_prop->KeyOrder.push_back(CollProp->KeyOrder[i]);
    return phys_prop;
  }
}

Cost *FILE_SCAN::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float Card = ((LOG_COLL_PROP *)LocalLogProp)->Card;
  float Width = ((LOG_COLL_PROP *)LocalLogProp)->Schema->GetTableWidth(0);
  // cost = 表体积 * 读取cost
  Cost *Result = new Cost(ceil(Card * Width) * (costModel->cpu_read() +  // cpu cost of reading from disk
                                                costModel->io())         // i/o cost of reading from disk
  );
  return (Result);
}

LOOPS_JOIN::LOOPS_JOIN(int *lattrs, int *rattrs, int size) : lattrs(lattrs), rattrs(rattrs), size(size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOOPS_JOIN].New();
  name = GetName();
}

LOOPS_JOIN::LOOPS_JOIN(LOOPS_JOIN &Op)
    : lattrs(CopyArray(Op.lattrs, Op.size)), rattrs(CopyArray(Op.rattrs, Op.size)), size(Op.size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOOPS_JOIN].New();
  name = GetName();
};

Cost *LOOPS_JOIN::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float LeftCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float RightCard = ((LOG_COLL_PROP *)InputLogProp[1])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  // 表join条件计算和数据copy的代价
  // 如果是overflow的情况，需要计算io|  如果是分布式，需要计算网络IO
  Cost *result = new Cost(LeftCard * RightCard * costModel->cpu_pred()  // cpu cost of predicates
                          + OutputCard * costModel->touch_copy()        // cpu cost of copying result
  );

  return (result);
}

// requirement is: left input must have the same props as the output,
//					while right input has no required prop
// nest loop join
PHYS_PROP *LOOPS_JOIN::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (PhysProp->GetOrder() != any)  // check possibility: satisfied output prop
  {
    if (InputNo == 0)  // left input's schema should include the prop keys
    {
      if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
        possible = false;
        return nullptr;
      }
    }
  }

  possible = true;

  if (InputNo == 0)  // pass the prop to left input
    return new PHYS_PROP(*PhysProp);
  else  // no reqd prop for right input
    return new PHYS_PROP(any);
}

string LOOPS_JOIN::Dump() {
  string os;
  int i;

  os = GetName() + "(<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(lattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(lattrs[i]) + ">,<";
  else
    os += ">,<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(rattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(rattrs[i]) + ">)";
  else
    os += ">)";

  return os;
};

PDUMMY::PDUMMY() { name = GetName(); }

PDUMMY::PDUMMY(PDUMMY &Op) { name = GetName(); };

// Imitate LOOPS_JOIN - why not?
Cost *PDUMMY::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float LeftCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float RightCard = ((LOG_COLL_PROP *)InputLogProp[1])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  Cost *result = new Cost(LeftCard * RightCard * costModel->cpu_pred()  // cpu cost of predicates
                          + OutputCard * costModel->touch_copy()        // cpu cost of copying result
                                                                        // no i/o cost
  );

  return (result);
}  // FindLocalCost

// requirement is: left input must have the same props as the output,
//					while right input has no required prop
PHYS_PROP *PDUMMY::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (PhysProp->GetOrder() != any)  // check possibility: satisfied output prop
  {
    if (InputNo == 0)  // left input's schema should include the prop keys
    {
      if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
        possible = false;
        return NULL;
      }
    }
  }

  possible = true;

  if (InputNo == 0)  // pass the prop to left input
    return new PHYS_PROP(*PhysProp);
  else  // no reqd prop for right input
    return new PHYS_PROP(any);
}

string PDUMMY::Dump() { return GetName(); };

/*
Nested loops index join
=======================
*/

LOOPS_INDEX_JOIN::LOOPS_INDEX_JOIN(int *lattrs, int *rattrs, int size, int CollId)
    : lattrs(lattrs), rattrs(rattrs), size(size), CollId(CollId) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOOPS_INDEX_JOIN].New();
  name = GetName();
}  // LOOPS_INDEX_JOIN::LOOPS_INDEX_JOIN

LOOPS_INDEX_JOIN::LOOPS_INDEX_JOIN(LOOPS_INDEX_JOIN &Op)
    : lattrs(CopyArray(Op.lattrs, Op.size)), rattrs(CopyArray(Op.rattrs, Op.size)), size(Op.size), CollId(Op.CollId) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOOPS_INDEX_JOIN].New();
  name = GetName();
};

Cost *LOOPS_INDEX_JOIN::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float LeftCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float RightCard = Cat->GetCollProp(CollId)->Card;
  float RightWidth = Cat->GetCollProp(CollId)->Width;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  Cost *result = new Cost(LeftCard * costModel->index_probe()  // cpu cost of finding index
                          + OutputCard                         // number of result tuples
                                * (2 * costModel->cpu_read()   // cpu cost of reading right index and result
                                   + costModel->touch_copy())  // cpu cost of copying left result
                          + MIN(LeftCard, ceil(RightCard / costModel->index_bf()))  // number of index blocks
                                * costModel->io()                                   // i/o cost of reading right index
                          + MIN(OutputCard, ceil(RightCard * RightWidth))           // number of result blocks
                                * costModel->io()                                   // i/o cost of reading right result
  );

  return (result);
}  // FindLocalCost

//##ModelId=3B0C086F0064
PHYS_PROP *LOOPS_INDEX_JOIN::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  assert(InputNo == 0);  // only one input

  if (PhysProp->GetOrder() != any)  // check possibility: satisfied output prop
  {
    if (InputNo == 0)  // left input's schema should include the prop keys
    {
      if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
        possible = false;
        return NULL;
      }
    }
  }

  possible = true;

  // pass the prop to inputs
  return new PHYS_PROP(*PhysProp);
}

string LOOPS_INDEX_JOIN::Dump() {
  string os;
  int i;

  os = GetName() + "(<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(lattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(lattrs[i]) + ">,<";
  else
    os += ">,<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(rattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(rattrs[i]) + ">)";
  else
    os += ">)";

  os += "Index on " + GetCollName(CollId);

  return os;
};

/*
Merge join
==========
*/

MERGE_JOIN::MERGE_JOIN(int *lattrs, int *rattrs, int size) : lattrs(lattrs), rattrs(rattrs), size(size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_MERGE_JOIN].New();
  name = GetName();
}  // MERGE_JOIN::MERGE_JOIN

MERGE_JOIN::MERGE_JOIN(MERGE_JOIN &Op)
    : lattrs(CopyArray(Op.lattrs, Op.size)), rattrs(CopyArray(Op.rattrs, Op.size)), size(Op.size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_MERGE_JOIN].New();
  name = GetName();
};

Cost *MERGE_JOIN::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float LeftCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float RightCard = ((LOG_COLL_PROP *)InputLogProp[1])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  Cost *result = new Cost((LeftCard + RightCard) * costModel->cpu_pred()  // cpu cost of predicates
                          + OutputCard * costModel->touch_copy()          // cpu cost of copying result
                                                                          // no i/o cost
  );

  return (result);
}  // FindLocalCost

string MERGE_JOIN::Dump() {
  string os;
  int i;

  os = GetName() + "(<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(lattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(lattrs[i]) + ">,<";
  else
    os += ">,<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(rattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(rattrs[i]) + ">)";
  else
    os += ">)";

  return os;
};

// inputs must be sorted
PHYS_PROP *MERGE_JOIN::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (PhysProp->GetOrder() != any)  // If specific output property is required
  {
    if (!PhysProp->Keys->Equal(lattrs, size) && !PhysProp->Keys->Equal(rattrs, size)) {
      possible = false;
      return NULL;
    }
  }

  possible = true;

  KEYS_SET *Keys;
  if (InputNo == 0) Keys = new KEYS_SET(lattrs, size);
  if (InputNo == 1) Keys = new KEYS_SET(rattrs, size);

  PHYS_PROP *result = new PHYS_PROP(Keys, sorted);  // require sort on input
  // the KeyOrder is ascending for all keys
  for (int i = 0; i < size; i++) result->KeyOrder.push_back(ascending);

  return result;

}  // MERGE_JOIN::InputReqdProp

// the physprop of the output is sorted on lattrs, rattrs, in the order of
// attrs of the EQJOIN operator
PHYS_PROP *MERGE_JOIN::FindPhysProp(PHYS_PROP **input_phys_props) {
  KEYS_SET *Keys, *Keys2;
  Keys = new KEYS_SET(lattrs, size);
  Keys2 = new KEYS_SET(rattrs, size);
  Keys->Merge(*Keys2);

  PHYS_PROP *result = new PHYS_PROP(Keys, sorted);  // sorted on lattrs, and rattrs
  // the KeyOrder is ascending for all keys
  for (int i = 0; i < size + size; i++) result->KeyOrder.push_back(ascending);

  return result;
}  // MERGE_JOIN::FindPhysProp

/*
HASH join
=========
       Like Merge join, but inputs can have any property.  Operator constructs a hash table.
*/

HASH_JOIN::HASH_JOIN(int *lattrs, int *rattrs, int size) : lattrs(lattrs), rattrs(rattrs), size(size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_HASH_JOIN].New();
  name = GetName();
}  // HASH_JOIN::HASH_JOIN

HASH_JOIN::HASH_JOIN(HASH_JOIN &Op)
    : lattrs(CopyArray(Op.lattrs, Op.size)), rattrs(CopyArray(Op.rattrs, Op.size)), size(Op.size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_HASH_JOIN].New();
  name = GetName();
};

Cost *HASH_JOIN::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float LeftCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float RightCard = ((LOG_COLL_PROP *)InputLogProp[1])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  Cost *result = new Cost(RightCard * costModel->hash_cost()      // cpu cost of building hash table
                          + LeftCard * costModel->hash_probe()    // cpu cost of finding hash bucket
                          + OutputCard * costModel->touch_copy()  // cpu cost of copying result
  );                                                              // no i/o cost

  return (result);
}  // FindLocalCost

string HASH_JOIN::Dump() {
  string os;
  int i;

  os = GetName() + "(<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(lattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(lattrs[i]) + ">,<";
  else
    os += ">,<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(rattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(rattrs[i]) + ">)";
  else
    os += ">)";

  return os;
};

// No properties are required of inputs
//##ModelId=3B0C086F026B
PHYS_PROP *HASH_JOIN::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (PhysProp->GetOrder() != any)  // check possibility: satisfied output prop
  {
    if (InputNo == 0)  // left input's schema should include the prop keys
    {
      if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
        possible = false;
        return NULL;
      }
    }
  }

  possible = true;

  if (InputNo == 0)  // pass the prop to left input
    return new PHYS_PROP(*PhysProp);
  else  // no reqd prop for right input
    return new PHYS_PROP(any);

}  // HASH_JOIN::InputReqdProp

/*
Filter
======
*/
//##ModelId=3B0C08700050
Cost *FILTER::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  // Need to have a cost for 0 tuples case	+ 1 ??
  Cost *result = new Cost(InputCard * costModel->cpu_pred()       // cpu cost of predicates
                          + OutputCard * costModel->touch_copy()  // cpu cost of copying result
                                                                  // no i/o cost
  );

  return (result);
}  // FindLocalCost

//##ModelId=3B0C0870005C
PHYS_PROP *FILTER::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (InputNo == 1)  // right input is Item Group, no reqd prop for it
  {
    possible = true;
    return (new PHYS_PROP(any));
    // return NULL;
  }

  // else, for left input
  if (PhysProp->GetOrder() != any)  // check possibility: satisfied output prop
  {
    if (InputNo == 0)  // left input's schema should include the prop keys
    {
      if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
        possible = false;
        return NULL;
      }
    }
  }

  possible = true;

  // pass the prop to inputs
  return (new PHYS_PROP(*PhysProp));
}

P_PROJECT::P_PROJECT(int *attrs, int size) : attrs(attrs), size(size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_P_PROJECT].New();
  name = GetName();
}  // P_PROJECT::P_PROJECT

P_PROJECT::P_PROJECT(P_PROJECT &Op) : attrs(CopyArray(Op.attrs, Op.size)), size(Op.size) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_P_PROJECT].New();
  name = GetName();
};

//##ModelId=3B0C086F0367
Cost *P_PROJECT::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  assert(InputCard == OutputCard);

  // Need to have a cost for 0 tuples case	+ 1 ??
  Cost *result = new Cost(InputCard * costModel->touch_copy()  // cpu cost of copying result
                                                               // no i/o cost
  );

  return (result);
}  // FindLocalCost

//##ModelId=3B0C086F0372
PHYS_PROP *P_PROJECT::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  assert(InputNo == 0);  // only one input

  if (PhysProp->GetOrder() != any)  // check possibility: satisfied output prop
  {
    if (InputNo == 0)  // left input's schema should include the prop keys
    {
      if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
        possible = false;
        return NULL;
      }
    }
  }

  possible = true;

  // pass the prop to inputs
  return new PHYS_PROP(*PhysProp);
}

string P_PROJECT::Dump() {
  string os;
  int i;

  os = GetName() + "(";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(attrs[i]) + ",";

  os += GetAttName(attrs[i]) + ")";

  return os;
}  // P_PROJECT::Dump

/*
  Sorting
  =======
*/

QSORT::QSORT() {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_QSORT].New();
  name = GetName();
}  // QSORT::QSORT

QSORT::QSORT(QSORT &Op) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_QSORT].New();
  name = GetName();
};

string QSORT::Dump() { return GetName(); }  // QSORT::Dump

Cost *QSORT::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  // very very bogus
  // double card = MAX(1, input_card);	// bogus NaN error
  // double card = MAX(1, 10000 * (1/input_card));	// bogus NaN error
  float card = MAX(1, OutputCard);

  Cost *result = new Cost(2 * card * log(card) / log(2.0)  // number of comparison and move
                          * costModel->cpu_comp_move()     // cpu cost of compare and move
                                                           // no i/o cost
  );

  return (result);
}  // QSORT::FindLocalCost

//##ModelId=3B0C0870015F
PHYS_PROP *QSORT::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (PhysProp->GetOrder() == any) {
    possible = false;  // enforcer does not satisfy the ANY prop
    return NULL;
  }

  if (PhysProp->GetOrder() == sorted) {
    // check the keys
    if (((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys))
      possible = true;
    else
      possible = false;
  } else
    assert(false);

  return new PHYS_PROP(any);
  // return NULL;	// any input will result in a sorted output

}  // QSORT::InputReqdProp

//##ModelId=3B0C0870023B
Cost *HASH_DUPLICATES::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  // Need to have a cost for 0 tuples case	+ 1 ??
  Cost *result = new Cost(InputCard * costModel->hash_cost()      // cpu cost of hashing
                                                                  // assume hash collisions add negligible cost
                          + OutputCard * costModel->touch_copy()  // cpu cost of copying result
                                                                  // no i/o cost
  );

  return (result);
}  // FindLocalCost

//##ModelId=3B0C08700245
PHYS_PROP *HASH_DUPLICATES::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  assert(InputNo == 0);  // one input
  possible = true;
  // pass the prop to inputs
  return new PHYS_PROP(*PhysProp);
}

//##ModelId=3B0C08700263
string HASH_DUPLICATES::Dump() { return GetName(); }  // HASH_DUPLICATES::Dump

// this cost does not model the cost of the average aggregate function very well
// since it actually requires more than one pass.
// One pass to group, count and sum. and one pass to divide sum by count
//##ModelId=3B0C087003A3
Cost *HGROUP_LIST::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  // Need to have a cost for 0 tuples case	+ 1 ??
  Cost *result = new Cost(InputCard * (costModel->hash_cost()                        // cost of hashing
                                       + costModel->cpu_apply() * (AggOps->size()))  // apply the aggregate operation
                          + OutputCard * (costModel->touch_copy())                   // copy out the result
  );

  return (result);
}  // HGROUP_LIST::FindLocalCost

//##ModelId=3B0C087003AD
PHYS_PROP *HGROUP_LIST::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  assert(InputNo == 0);  // one input

  possible = true;
  // pass the prop to inputs
  return new PHYS_PROP(*PhysProp);
}

// the physprop of the output is sorted on Gby attrs
//##ModelId=3B0C087003CC
PHYS_PROP *HGROUP_LIST::FindPhysProp(PHYS_PROP **input_phys_props) {
  KEYS_SET *Keys;
  Keys = new KEYS_SET(GbyAtts, GbySize);

  PHYS_PROP *result = new PHYS_PROP(Keys, sorted);  // sorted on group_by attributes
  for (int i = 0; i < GbySize; i++) result->KeyOrder.push_back(ascending);

  return result;
}  // HGROUP_LIST::FindPhysProp

string HGROUP_LIST::Dump() {
  string os;
  int i;

  os = GetName() + "( Group By:";

  for (i = 0; (i < GbySize - 1); i++) os += GetAttName(GbyAtts[i]) + ",";

  if (GbySize > 0)
    os += GetAttName(GbyAtts[i]) + " )";
  else
    os += "Empty set )";

  // dump AggOps
  os += "( Aggregating: ";
  int NumOps = AggOps->size();
  for (i = 0; i < NumOps - 1; i++) {
    os += (*AggOps)[i]->Dump();
  }
  if (NumOps > 0) {
    os += (*AggOps)[i]->Dump();
    os += " )";
  } else {
    os += "Empty set )";
  }

  return os;
}  // HGROUP_LIST::Dump

//##ModelId=3B0C0871011B
Cost *P_FUNC_OP::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;
  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  // Need to have a cost for 0 tuples case	+ 1 ??
  Cost *result = new Cost(InputCard * costModel->cpu_apply()      // cpu cost of applying aggregate operation
                          + OutputCard * costModel->touch_copy()  // copy out the result
  );
  return (result);
}  // P_FUNC_OP::FindLocalCost

PHYS_PROP *P_FUNC_OP::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  assert(InputNo == 0);  // one input
  possible = true;
  // pass the prop to inputs
  return new PHYS_PROP(*PhysProp);
}

string P_FUNC_OP::Dump() {
  string os;
  int i;

  os = GetName() + "(";

  for (i = 0; (AttsSize > 0) && (i < AttsSize - 1); i++) os += GetAttName(Atts[i]) + ",";

  os += GetAttName(Atts[i]) + ")";

  os += " AS " + RangeVar;
  return os;
}

BIT_JOIN::BIT_JOIN(int *lattrs, int *rattrs, int size, int CollId)
    : lattrs(lattrs), rattrs(rattrs), size(size), CollId(CollId) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_BIT_JOIN].New();
  name = GetName();
}  // BIT_JOIN::BIT_JOIN

BIT_JOIN::BIT_JOIN(BIT_JOIN &Op)
    : lattrs(CopyArray(Op.lattrs, Op.size)), rattrs(CopyArray(Op.rattrs, Op.size)), size(Op.size), CollId(Op.CollId) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOOPS_INDEX_JOIN].New();
  name = GetName();
};

Cost *BIT_JOIN::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float LeftCard = ((LOG_COLL_PROP *)InputLogProp[0])->Card;

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;

  Cost *result = new Cost(LeftCard * costModel->cpu_read()    // cpu cost of reading bit vector
                          + LeftCard * costModel->cpu_pred()  // cpu cost of check bit vector
                                                              // the above is overstated:
                                                              //	1. The read assumes we read 1
                                                              //	   bit at a time
                                                              //	2. cost of evaluating a predicate
                                                              //	   is just checking a single bit
                                                              //		+OutputCard
                                                              //// number of result tuples
                                                              //		 * costModel->touch_copy()
                                                              //// cpu cost of projecting and
                                                              // copying result
                          + (LeftCard / costModel->bit_bf())  // number of bit vector blocks
                                * costModel->io()             // i/o cost of reading bit vector
  );

  return (result);
}  // BIT_JOIN::FindLocalCost

PHYS_PROP *BIT_JOIN::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  if (InputNo == 1)  // no reqd prop for right input
  {
    possible = true;
    return new PHYS_PROP(any);
  }

  if (PhysProp->GetOrder() != any)  // If specific output property is required
  {
    if (!((LOG_COLL_PROP *)InputLogProp)->Schema->Contains(PhysProp->Keys)) {
      possible = false;
      return NULL;
    }
  }

  possible = true;

  return new PHYS_PROP(*PhysProp);
}

string BIT_JOIN::Dump() {
  string os;
  int i;

  os = GetName() + "(<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(lattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(lattrs[i]) + ">,<";
  else
    os += ">,<";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(rattrs[i]) + ",";

  if (size > 0)
    os += GetAttName(rattrs[i]) + ">)";
  else
    os += ">)";

  os += "Using bit Index on " + GetCollName(CollId);

  return os;
};

// the physprop of the output is the left input prop
//##ModelId=3B0C087102F1
PHYS_PROP *BIT_JOIN::FindPhysProp(PHYS_PROP **input_phys_props) {
  return input_phys_props[0];
}  // BIT_JOIN::FindPhysProp

// INDEXED_FILTER
INDEXED_FILTER ::INDEXED_FILTER(const int fileId) : FileId(fileId) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_INDEXED_FILTER].New();
  name = GetName() + GetCollName(FileId);
};

INDEXED_FILTER::INDEXED_FILTER(INDEXED_FILTER &Op) : FileId(Op.GetFileId()) {
  if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_INDEXED_FILTER].New();
  name = Op.name;
}

Cost *INDEXED_FILTER::FindLocalCost(LOG_PROP *LocalLogProp, LOG_PROP **InputLogProp) {
  float InputCard = Cat->GetCollProp(FileId)->Card;
  float Width = Cat->GetCollProp(FileId)->Width;
  INT_ARRAY *Indices = Cat->GetIndNames(FileId);

  float OutputCard = ((LOG_COLL_PROP *)LocalLogProp)->Card;
  KEYS_SET FreeVar = ((LOG_ITEM_PROP *)InputLogProp[0])->FreeVars;

  double data_cost, index_cost;
  double pred_cost = 0;

  bool Clustered = false;

  // for model D, redicate must be on a single attribute to use this physical operator.
  assert(FreeVar.GetSize() == 1);

  for (int i = 0; i < Indices->size(); i++) {
    int IndexId = Indices->at(i);
    if (IndexId == FreeVar[0]                           // FreeVar is contained in index list
        && Cat->GetIndProp(IndexId)->IndType == btree   // index type is btree
        && Cat->GetIndProp(IndexId)->Clustered == true  // index is clustered
    ) {
      Clustered = true;
      break;
    }
  }

  if (Clustered) {
    /*
     * BTREE clustered
     *      (assuming a single range predicate e.g. 8 < value < 23 )
     *
     *      Algorithm:
     *      Lookup FIRST tuple in range using index.
     *      Read all tuples from that point on, evaluating predicate
     *      on each tuple, until encounter tuple where predicate
     *      (e.g. value < 23) is FALSE
     */
    // single read index
    index_cost = costModel->cpu_read()  // cpu cost of reading index from disk;
                 + costModel->io();     // io cost of reading index from disk
    // read data until predicate fails
    data_cost = ceil(OutputCard * Width) * (costModel->cpu_read()  // cpu cost of reading data from disk
                                            + costModel->io());    // io cost of reading data from disk
    // evaluate predicate until predicate fails
    pred_cost = OutputCard * costModel->cpu_pred();
  } else {
    /*
     * Not clustered
     *
     *      Algorithm:
     *      Lookup EACH tuple in range (satisfying predicate) using index.
     *
     *      (if small number of resulting tuples)
     *      For each tuple in index:
     *          Read the block the tuple is on.
     *
     *      OR
     *
     *      (if large number of resulting tuples)
     *      Gather all pointers to resulting tuples
     *      Read each block of file, retrieve resulting tuples
     */
    // read as many index blocks as necessary
    index_cost = ceil(OutputCard / costModel->index_bf()) * (costModel->cpu_read() + costModel->io());
    // read all data blocks or read data for every index entry found
    data_cost = MIN(ceil(InputCard * Width), OutputCard) * (costModel->cpu_read() + costModel->io());
  }

  Cost *Result = new Cost(index_cost + data_cost + pred_cost);

  return (Result);
}

//##ModelId=3B0C0872005D
PHYS_PROP *INDEXED_FILTER::InputReqdProp(PHYS_PROP *PhysProp, LOG_PROP *InputLogProp, int InputNo, bool &possible) {
  assert(InputNo == 0);  // one input
  possible = true;
  // no requirement for inputs
  return new PHYS_PROP(any);
  // return( NULL );
}
