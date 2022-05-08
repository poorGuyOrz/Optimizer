// SUPP.H - Classes which support optimization and are used throughout.

#pragma once

#include "../header/stdafx.h"  // general definitions

class OPT_STAT;  // opt statistics

// Properties of stored objects, including physical and logical properties.
class CollectionsProperties;  // Collections
class IndexProperties;        // Indexes

class Attribute;      // Attribute
class Schema;         // Includes a set of attributes, some may be equivalent
class LOG_PROP;       // Abstract class of logical properties.
class LOG_COLL_PROP;  // For collection types
class LOG_ITEM_PROP;  // For items (predicates)
class PHYS_PROP;      // Physical Properties
class CONT;           // Context: Conditions/Constraints on a search
class Cost;           // Cost of a physical operator or expression

// other statistics
class OPT_STAT {
 public:
  int TotalMExpr;
  int DupMExpr;
  int HashedMExpr;
  int MaxBucket;
  int FiredRule;

  OPT_STAT() : TotalMExpr(0), DupMExpr(0), FiredRule(0), HashedMExpr(0), MaxBucket(0){};

  string Dump() {
    string os;
    os += "Duplicate MExpr: " + to_string(DupMExpr) + "\n";
    os += "Hashed Logical MExpr: " + to_string(HashedMExpr) + "\n";
    os += "Max Overflow Buckets: " + to_string(MaxBucket) + "\n";
    os += "FiredRules: " + to_string(FiredRule) + "\n";

    return os;
  };

};  // class OPT_STAT

/*
    ============================================================
    ORDERED SET OF ATTRIBUTES - class KEYS_SET
    ============================================================
*/
// Used for keys when sorted, hashed.  That's why order matters.
// Also used for conditions (cf. eqjoin)
class KEYS_SET {
 private:
  // A set of attribute names
  vector<int> KeyArray;

 public:
  KEYS_SET(){};

  KEYS_SET(int *array, int size) {
    KeyArray.resize(size);
    for (int i = 0; i < size; i++) KeyArray[i] = array[i];
  }

  KEYS_SET(KEYS_SET &other)  // copy constructor
  {
    KeyArray = (other.KeyArray);
  };

  KEYS_SET &operator=(KEYS_SET &other)  //  = operator
  {
    KeyArray = (other.KeyArray);
    return *this;
  };

  ~KEYS_SET(){};

  // return FALSE if duplicate found, and don't add it to the ordered set.
  bool AddKey(string CollName, string KeyName);
  bool AddKey(int AttId);

  // check if the attid is in the keys_Set
  bool ContainKey(int AttId);

  // Transform each element from A.B to NewName.B (actually the IDs)
  void update(string NewName);

  // return int array from the keys_set
  int *CopyOut();

  // return int array of size one from the keys_set
  int *CopyOutOne(int i);

  // merge keys, ignore duplicates
  inline void Merge(KEYS_SET &other) {
    for (int i = 0; i < other.GetSize(); i++) AddKey(other[i]);
  }

  // return the string in the order Set
  inline int &operator[](int n) { return KeyArray[n]; }

  // return the number of the keys
  inline int GetSize() { return KeyArray.size(); }

  // decrements the size of key array
  inline void SetSize() { KeyArray.resize(GetSize() - 1); }

  // sets the size of key array
  inline void SetSize(int newsize) { KeyArray.resize(newsize); }

  bool operator==(KEYS_SET &other)  //  = operator
  {
    if (GetSize() != other.GetSize()) return false;

    for (int i = 0; i < GetSize(); i++)
      if (KeyArray[i] != other[i]) return false;

    return true;  // they are identical
  };

  bool Equal(int *array, int size) {
    if (GetSize() != size) return false;

    for (int i = 0; i < size; i++)
      if (KeyArray[i] != array[i]) return false;

    return true;  // they are identical
  }

  bool IsSubSet(int *array, int size) {
    if (GetSize() > size) return false;
    int i, j;

    for (i = 0; i < GetSize(); i++)
      for (j = 0; j < size; j++)
        if (KeyArray[i] == array[j]) break;
    if (j == size) return false;  // not in array

    return true;  // this keys_set is contained in array
  }

  // Remove the KEYS_SET with AttId = val
  bool RemoveKeysSet(int val) {
    if (val > GetSize()) return false;

    for (int k = val; k < GetSize() - 1; k++) KeyArray[k] = KeyArray[k + 1];

    SetSize();

    return true;
  }

  // choose the attribute with max cucard value
  int ChMaxCuCard();
  // get the cucard of the attribute
  float GetAttrCuCard(int);
  string Dump();
  void reset() { KeyArray.resize(0); };
  // KEYS_SET* best();
};

// foreign key
class FOREIGN_KEY {
 public:
  KEYS_SET *ForeignKey;
  KEYS_SET *RefKey;

  FOREIGN_KEY(KEYS_SET *FKeys, KEYS_SET *RKeys) : ForeignKey(FKeys), RefKey(RKeys){};
  FOREIGN_KEY(FOREIGN_KEY &other)  // copy constructor
  {
    ForeignKey = new KEYS_SET(*other.ForeignKey);
    RefKey = new KEYS_SET(*other.RefKey);
  };

  ~FOREIGN_KEY() {
    delete ForeignKey;
    delete RefKey;
  }

  string Dump();

  void update(string NewName) { ForeignKey->update(NewName); }
};

/*
============================================================
PROPERTIES OF STORED OBJECTS- classes CollectionsProperties, ATT_PROP and IndexProperties
============================================================
*/
// Properties of Stored Collections
class CollectionsProperties {
 public:
  // Logical Properties
  float Card;   // Cardinality
  float UCard;  // Unique Cardinality
  float Width;  // width of the table, fraction of a block

  // Beware - this does not delete *Keys.
  CollectionsProperties &operator=(CollectionsProperties &other)  //  = operator
  {
    Keys = new KEYS_SET(*other.Keys);
    CandidateKey = new KEYS_SET(*other.CandidateKey);
    for (int i = 0; i < other.FKeyArray.size(); i++) {
      FOREIGN_KEY *fk = new FOREIGN_KEY(*other.FKeyArray[i]);
      FKeyArray.push_back(fk);
    }
    Card = other.Card;
    UCard = other.UCard;
    Width = other.Width;
    Order = other.Order;
    if (Order == sorted)
      for (int i = 0; i < other.KeyOrder.size(); i++) KeyOrder.push_back(other.KeyOrder[i]);
    return *this;
  };

  // Physical properties
  ORDER Order;     // any, heap, sorted or hashed
  KEYS_SET *Keys;  // Keys on which sorted or hashed
  // null if heap or any, nonnull otherwise
  KeyOrderArray KeyOrder;  // if order is sorted, nonnull
  // need ascending/descending for each key

  // the candidate keys
  KEYS_SET *CandidateKey;

  vector<FOREIGN_KEY *> FKeyArray;

  // initialize member with -1, i.e.,not known
  CollectionsProperties() : Card(-1), UCard(-1){};

  // Transform all keys in the properties to new name
  void update(string NewName);

  ~CollectionsProperties() {
    delete Keys;
    delete CandidateKey;
    for (int i = 0; i < FKeyArray.size(); i++) delete FKeyArray[i];
  };

  string Dump();

  // copy constructor
  CollectionsProperties(CollectionsProperties &other);

  // store the foreign key strings, translated to foreign keys at the end of CAT
  STRING_ARRAY ForeignKeyString;
};

// Index Properties
class IndexProperties {
 public:
  IndexProperties(){};
  ~IndexProperties() { delete Keys; };

  IndexProperties &operator=(IndexProperties &other)  //  = operator
  {
    Keys = new KEYS_SET(*other.Keys);
    IndType = other.IndType;
    Clustered = other.Clustered;
    return *this;
  };

  // Physical Properties
  KEYS_SET *Keys;       // Index is on this ordered set of attributes
  ORDER_INDEX IndType;  // hash or sort (Btree)
  bool Clustered;
  // Transform each key from A.X to NewName.X
  void update(string NewName);
  string Dump();
};

// Bit Index Properties
class BitIndexProperties {
 public:
  KEYS_SET *BitAttr;     // attributes for which all values are bit indexed
  int IndexAttr;         // indexing attributes -- this is the key of the table
                         // it is dense key. we assume it is integer type
                         // bit indexes are only allowed for tables with single
                         //  attibute keys
  string BitPredString;  // assume store the index for each predicate separately
  string IndexAttrString;
  BitIndexProperties(){};
  ~BitIndexProperties() { delete BitAttr; };
  void update(string NewName);
  string Dump();
  BitIndexProperties &operator=(BitIndexProperties &other)  //  = operator
  {
    BitAttr = new KEYS_SET(*other.BitAttr);
    IndexAttr = other.IndexAttr;
    return *this;
  };
};

/*
============================================================
Attribute - Attribute
============================================================
*/

class Attribute {
 public:
  int AttId;
  float CuCard;  // cardinality
  float Max;     // max, min value
  float Min;

  Attribute(){};

  Attribute(const int attId, const float CuCard, const float min, const float max)
      : AttId(attId), CuCard(CuCard), Min(min), Max(max){};

  Attribute(string range_var, int *atts, int size);

  Attribute(Attribute &other) : AttId(other.AttId), CuCard(other.CuCard), Min(other.Min), Max(other.Max){};

  ~Attribute(){};

  string Dump();
  string attrDump();
  string DumpCOVE();

};  // class Attribute

/*
   ============================================================
   Schema - Structure of a Group: attributes and their properties
   ============================================================
*/

// Think of each group as a temporary relation, which has a schema.  The schema
// is a set of attributes, plus information about which attributes are equivalent
// (attributes are made equivalent by joins on them).

// An attribute is represented by a path name plus the name of the attribute,
// e.g. Emp plus age.

class Schema {
 private:
  Attribute **Attrs;  // Attributes
  int Size;           // number of the attrs

 public:
  int *TableId;  // Base tables appearing in this schema - used for calculating Max cucards
  //  for each base table in a schema, and for calculating log prop of aggregate.
  int TableNum;  // number of the tables

 public:
  // Make space for n attrs
  Schema(int n) : Size(n) {
    assert(Size >= 0);
    Attrs = new Attribute *[Size];
  };

  Schema(Schema &other) : Size(other.Size), TableNum(other.TableNum) {
    int i;
    assert(Size >= 0);
    Attrs = new Attribute *[Size];
    for (i = 0; i < Size; i++) Attrs[i] = new Attribute(*(other[i]));
    TableId = new int[TableNum];
    for (i = 0; i < TableNum; i++) TableId[i] = other.GetTableId(i);
  }

  ~Schema();

  // return FALSE if duplicate found
  bool AddAttr(int Index, Attribute *attr);

  // return true if the attr is in the schema
  bool InSchema(int AttId);

  // return true if contains all the keys
  bool Contains(KEYS_SET *Keys);

  // 	projection of attrs onto schema
  Schema *projection(int *attrs, int size);

  // union the schema of the joined collection
  Schema *UnionSchema(Schema *other);

  // return the nth attr in the schema
  inline Attribute *operator[](int n) { return Attrs[n]; }

  inline int GetSize() { return Size; }

  // number of Tables in the schema
  inline int GetTableNum() { return TableNum; };

  // max cucard of each tables in the schema
  float GetTableMaxCuCard(int TableIndex);

  // width of each tables in the schema
  float GetTableWidth(int TableIndex);

  // CollId of the table
  inline int GetTableId(int TableIndex) { return TableId[TableIndex]; };

  // store the key-sets of all attributes in the schema
  KEYS_SET *AttrStore();

  string Dump();
  string DumpCOVE();

};  // class Schema

/*
   ============================================================
   class LOG_PROP - LOGICAL PROPERTIES
   ============================================================
*/

class LOG_PROP {
  // Abstract Class so an operator can deal with input logical properties of
  // all types of inputs: collection, item, and whatever else is defined.
 public:
  LOG_PROP(){};
  virtual ~LOG_PROP(){};

  virtual string Dump() = 0;
  virtual string DumpCOVE() = 0;
};

/*
============================================================
LOG_COLL_PROP: LOGICAL PROPERTIES OF COLLECTIONS
============================================================
*/
class LOG_COLL_PROP : public LOG_PROP {
 public:
  const float Card;      // Cardinality
  const float UCard;     // Unique Cardinality
  Schema *const schema;  // schema includes the column unique
  // cardinalities

  KEYS_SET *CandidateKey;  // candidate key

  vector<FOREIGN_KEY *> FKeyList;

  LOG_COLL_PROP(float card, float ucard, Schema *schema, KEYS_SET *cand_keys = NULL)
      : Card(card), UCard(ucard), schema(schema), CandidateKey(cand_keys){};

  /*	LOG_COLL_PROP(LOG_COLL_PROP & other) : Card(other.Card), UCard(other.UCard),
          Schema(other.Schema)
          {
          CandidateKey = NULL;
          if (other.CandidateKey)CandidateKey = new KEYS_SET(*other.CandidateKey);
          };
  */

  ~LOG_COLL_PROP() {
    delete schema;
    delete CandidateKey;
    for (int i = 0; i < FKeyList.size(); i++) delete FKeyList[i];
  };

  string Dump();
  string DumpCOVE();
};

class LOG_ITEM_PROP : public LOG_PROP {
 public:
  float Max;
  float Min;
  float CuCard;
  float Selectivity;
  KEYS_SET FreeVars;

 public:
  LOG_ITEM_PROP(float max, float min, float CuCard, float selectivity, KEYS_SET &freevars)
      : Max(max), Min(min), CuCard(CuCard), Selectivity(selectivity), FreeVars(freevars){};

  ~LOG_ITEM_PROP(){};

  string Dump() {
    string os;
    os = "Max : " + to_string(Max) + ", Min : " + to_string(Min) + ", CuCard : " + to_string(CuCard) +
         ", Selectivity : " + to_string(Selectivity) + ", FreeVars : " + FreeVars.Dump() + "\n";
    return os;
  };

  // Temporary, till we use LOG_ITEM_PROPs in COVE
  string DumpCOVE() {
    string os = "Error";
    // os.Format("%d %d %s%s%s \n",Card, UCard, "{", (*Schema).DumpCOVE(), "}");
    return os;
  };
};

/*
============================================================
PHYS_PROP: PHYSICAL PROPERTIES
============================================================
*/

// Physical properties of collections.  These properites
// distinguish collections which are logically equivalent.  Examples
// are orderings, data distribution, data compression, etc.

// Normally, a plan could have > 1 physical property.  For now,
//  we will work with hashing and sorting only, so we assume a plan
//  can have only one physical property.  Extensions should be
//  tedious but not too hard.

class PHYS_PROP {
 public:
  const ORDER Order;       // any, heap, sorted or hashed
  KEYS_SET *Keys;          // Keys on which sorted or hashed null if heap or any, nonnull otherwise
  KeyOrderArray KeyOrder;  // if order is sorted
                           // need ascending/descending for each key
  //	PHYS_PROP(KeyOrderArray *KeyOrder, KEYS_SET * Keys, ORDER Order);
  PHYS_PROP(KEYS_SET *Keys, ORDER Order);
  PHYS_PROP(ORDER Order);
  PHYS_PROP(PHYS_PROP &other);

  ~PHYS_PROP() {
    if (Order != any) delete Keys;
    // if (Order == sorted) delete [] KeyOrder;
  }

  ORDER GetOrder() { return (Order); }
  KEYS_SET *GetKeysSet() { return (Keys); }
  void SetKeysSet(KEYS_SET *NewKeys) { Keys = NewKeys; }

  // merge other phys_prop in
  void Merge(PHYS_PROP &other);
  void bestKey();  // returns the key and keyorder of the key
  // with the most distinct values

  bool operator==(PHYS_PROP &other);

  string Dump();
  string DumpCOVE();
};

/*
    ============================================================
    Cost - cost of executing a physical operator, expression or multiexpression
    ============================================================
Cost cannot be associated with a multiexpression, since cost is determined
by properties desired.  For example, a SELECT will cost
more if sorted is required.

Cost value of -1 = infinite cost.  Any other negative cost
   is considered an error.
*/

class Cost {
 private:
  double Value;  // Later this may be a base class specialized
                 // to various costs: CPU, IO, etc.
 public:
  Cost(double Number) : Value(Number) { assert(Number == -1 || Number >= 0); };
  Cost(Cost &other) : Value(other.Value){};

  ~Cost(){};

  // FinalCost() makes "this" equal to the total of local and input costs.
  //  It is an error if any input is null.
  //  In a parallel environment, this may involve max.
  void FinalCost(Cost *LocalCost, Cost **TotalInputCost, int Size);

  inline Cost &operator+=(const Cost &other) {
    if (Value == -1 || other.Value == -1)  // -1 means Infinite
      Value = -1;
    else
      Value += other.Value;

    return (*this);
  }

  inline Cost &operator*=(double EPS) {
    assert(EPS > 0);

    if (Value == -1)  // -1 means Infinite
      Value = 0;
    else
      Value *= EPS;

    return (*this);
  }

  inline Cost &operator/=(int arity) {
    assert(arity > 0);

    if (Value == -1)  // -1 means Infinite
      Value = 0;
    else
      Value /= arity;

    return (*this);
  }

  inline Cost &operator-=(const Cost &other) {
    if (Value == -1 || other.Value == -1)  // -1 means Infinite
      Value = -1;
    else
      Value -= other.Value;

    return (*this);
  }

  inline Cost &operator=(const Cost &other) {
    this->Value = other.Value;
    return (*this);
  }

  inline bool operator>=(const Cost &other) {
    if (Value == -1) return (true);

    if (other.Value == -1)  // -1 means Infinite
      return (false);

    return (this->Value >= other.Value);
  }

  inline Cost &operator*(double EPS) {
    assert(EPS >= 0);

    Cost *temp;
    if (Value == -1)  // -1 means Infinite
      temp = new Cost(0);
    else
      temp = new Cost(Value * EPS);
    return (*temp);
  }

  inline Cost &operator/(int arity) {
    assert(arity > 0);

    Cost *temp;
    if (Value == -1)  // -1 means Infinite
      temp = new Cost(0);
    else
      temp = new Cost(Value / arity);

    return (*temp);
  }

  inline bool operator>(const Cost &other) {
    if (Value == -1) return (true);

    if (other.Value == -1)  // -1 means Infinite
      return (false);

    return (this->Value > other.Value);
  }

  inline bool operator<(const Cost &other) {
    if (Value == -1) return (false);

    if (other.Value == -1)  // -1 means Infinite
      return (true);

    return (this->Value < other.Value);
  }

  string Dump();

};  // class Cost

/*
============================================================
CONTEXTs/CONSTRAINTS on a search
============================================================
*/
class CONT
// Each search for the cheapest solution to a problem or
// subproblem is done relative to some conditions, also called
// constraints.  In our context, a condition consists of
// required (not excluded) properties (e.g. must the solution be
// sorted) and an upper bound (e.g. must the solution cost less than 5).

// We are not using a lower bound.  It is not very effective.
// Each search spawns multiple subtasks, e.g. one task to fire each rule for the search.
// These subtasks all share the search's CONT.  Sharing is done
// not only to save space, but to share information about when
// the search is done, what is the current upper bound, etc.

{
 public:
  // The vector of contexts, vc, implements sharing.  Each task which
  // creates a context  adds an entry to this vector.  Finish is true
  //  means the task is done.
  static vector<CONT *> vc;

 private:
  PHYS_PROP *ReqdPhys;
  Cost *UpperBd;
  bool Finished;

 public:
  CONT(PHYS_PROP *, Cost *Upper, bool done);

  ~CONT() {
    delete UpperBd;
    delete ReqdPhys;
  };

  inline PHYS_PROP *GetPhysProp() { return (ReqdPhys); };
  inline Cost *GetUpperBd() { return (UpperBd); };
  inline void SetPhysProp(PHYS_PROP *RP) { ReqdPhys = RP; };

  string Dump() {
    return "Prop: " + ReqdPhys->Dump() + ", UpperBd: " + UpperBd->Dump() + ", Finished:" + to_string(Finished);
  }

  // set the flag if the context is done, means we completed the search,
  // may got a final winner, or found out optimal plan for this context not exist
  inline void done() { Finished = true; };
  inline bool is_done() { return Finished; };

  //  Update bounds, when we get better ones.
  inline void SetUpperBound(Cost &NewUB) { *UpperBd = NewUB; };

};  // class CONT

/*
           ============================================================
           Miscellaneous Functions
           ============================================================
*/
// return the int array whose attrs=attr-part_attr
int *GetOtherAttr(int *attr, int size, int *part_attr, int part_size);

// return the copy the int array
int *CopyArray(int *IntArray, int Size);

// return true if the contents of two arrays are equal
bool EqualArray(int *array1, int *array2, int size);

void bit_on(BIT_VECTOR &bit_vect, int rule_no);  // Turn this bit on

bool is_bit_off(BIT_VECTOR bit_vect, int rule_no);  // Is this bit off?

// need for group pruning, calculate the copy-out cost of the expr
double TouchCopyCost(LOG_COLL_PROP *LogProp);

// for cucard pruning, calculate the minimon cost of fetching cucard tuples from disc
double FetchingCost(LOG_COLL_PROP *LogProp);

// needed for hashing, used for duplicate elimination.  See ../doc/dupelim
ub4 lookup2(string k, ub4 length, ub4 initval);
ub4 lookup2(ub4 k, ub4 initval);

// Get the names from ids
string GetCollName(int CollId);
string GetAttName(int AttId);
string GetIndName(int IndId);
string GetBitIndName(int BitIndId);
// Transform A.B into B
string TruncName(string AttId);

// Get the Ids from names
int GetCollId(int AttId);
int GetCollId(string CollName);
int GetAttId(string CollName, string AttName);
int GetAttId(string Name);
int GetIndId(string CollName, string IndName);
int GetBitIndId(string CollName, string IndName);

// convert string to Domain type
DOM_TYPE atoDomain(char *p);

// conver string to ORDER_AD type
ORDER_AD atoKeyOrder(char *p);

// convert string to ORDER type
ORDER atoCollOrder(char *p);

// convert string to ORDER_INDEX type
ORDER_INDEX atoIndexOrder(char *p);

// convert Domain type to string
string DomainToString(DOM_TYPE p);

// convert Domain type to string
string OrderToString(ORDER p);

// convert Domain type to string
string IndexOrderToString(ORDER_INDEX p);

// return only the file name (without path)
string Trim(string pathname);

// skip the spaces in front of the string
char *SkipSpace(char *p);

// return true if the string line is Comment or blank line
bool IsCommentOrBlankLine(char *p);

// get a string from the line buf. (seperated by space char)
void parseString(char *p);
