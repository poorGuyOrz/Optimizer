// supp.cpp -  implementation of supplement classes

#include "../header/cat.h"
#include "../header/defs.h"
#include "../header/item.h"
#include "../header/stdafx.h"

//*************  Hash Function ******************
// needed for hashing, used for duplicate elimination.  See ../doc/dupelim

#define mix(a, b, c) \
  {                  \
    a -= b;          \
    a -= c;          \
    a ^= (c >> 13);  \
    b -= c;          \
    b -= a;          \
    b ^= (a << 8);   \
    c -= a;          \
    c -= b;          \
    c ^= (b >> 13);  \
    a -= b;          \
    a -= c;          \
    a ^= (c >> 12);  \
    b -= c;          \
    b -= a;          \
    b ^= (a << 16);  \
    c -= a;          \
    c -= b;          \
    c ^= (b >> 5);   \
    a -= b;          \
    a -= c;          \
    a ^= (c >> 3);   \
    b -= c;          \
    b -= a;          \
    b ^= (a << 10);  \
    c -= a;          \
    c -= b;          \
    c ^= (b >> 15);  \
  }

ub4 lookup2(ub4 k,        // the key to be hashed
            ub4 initval)  // the previous hash, or an arbitrary value
{
  ub4 a, b, c;

  /* Set up the internal state */
  a = b = 0x9e3779b9; /* the golden ratio; an arbitrary value */
  c = initval;        /* the previous hash value */

  a += (k << 8);
  b += (k << 16);
  c += (k << 24);
  mix(a, b, c);

  return c;
}

ub4 lookup2(string k,     // the key to be hashed
            ub4 length,   // the length of the key
            ub4 initval)  // the previous hash, or an arbitrary value
{
  ub4 a, b, c, len;

  /* Set up the internal state */
  len = length;
  a = b = 0x9e3779b9; /* the golden ratio; an arbitrary value */
  c = initval;        /* the previous hash value */
  int i = 0;          // How many bytes of k have we processed so far?

  /*---------------------------------------- handle most of the key */
  while (len >= 12) {
    a += (k[i + 0] + ((ub4)k[i + 1] << 8) + ((ub4)k[i + 2] << 16) + ((ub4)k[i + 3] << 24));
    b += (k[i + 4] + ((ub4)k[i + 5] << 8) + ((ub4)k[i + 6] << 16) + ((ub4)k[i + 7] << 24));
    c += (k[i + 8] + ((ub4)k[i + 9] << 8) + ((ub4)k[i + 10] << 16) + ((ub4)k[i + 11] << 24));
    mix(a, b, c);
    i += 12;
    len -= 12;
  }

  /*------------------------------------- handle the last 11 bytes */
  c += length;
  switch (len) /* all the case statements fall through */
  {
    case 11:
      c += ((ub4)k[i + 10] << 24);
    case 10:
      c += ((ub4)k[i + 9] << 16);
    case 9:
      c += ((ub4)k[i + 8] << 8);
      /* the first byte of c is reserved for the length */
    case 8:
      b += ((ub4)k[i + 7] << 24);
    case 7:
      b += ((ub4)k[i + 6] << 16);
    case 6:
      b += ((ub4)k[i + 5] << 8);
    case 5:
      b += k[i + 4];
    case 4:
      a += ((ub4)k[i + 3] << 24);
    case 3:
      a += ((ub4)k[i + 2] << 16);
    case 2:
      a += ((ub4)k[i + 1] << 8);
    case 1:
      a += k[i + 0];
      /* case 0: nothing left to add */
  }
  mix(a, b, c);
  /*-------------------------------------------- report the result */
  return c;
}

//*********** int array functions  *********

int *GetOtherAttr(int *attr, int size, int *part_attr, int part_size) {
  int result_size = size - part_size;
  int *result = new int[result_size];
  int i, j;
  // copy all the attributes excepts for those are candidate key
  for (i = 0; i < size; i++) {
    for (j = 0; j < part_size; j++)
      if (attr[i] == part_attr[j]) break;
    if (j == part_size) result[i] = attr[i];
  }

  return result;
}

// return the copy of the int array
int *CopyArray(int *IntArray, int Size) {
  int *result = new int[Size];
  memcpy(result, IntArray, Size * sizeof(int));
  return result;
}

// return true if the contents of two arrays are equal
bool EqualArray(int *array1, int *array2, int size) {
  for (int i = 0; i < size; i++)
    if (array1[i] != array2[i]) return false;

  return true;
}

// **********  BIT_VECTOR function **********
void bit_on(BIT_VECTOR &bit_vect, int rule_no)  // Turn this bit on
{
  unsigned int n = (1 << rule_no);

  assert(rule_no >= 0 && rule_no < 32);
  // assert( (bit_vect & n) == 0 );  //Be sure this bit is off!

  bit_vect = bit_vect | n;
};

bool is_bit_off(BIT_VECTOR bit_vect, int rule_no)  // Is this bit off?
{
  unsigned int n = (1 << rule_no);

  if ((bit_vect & n) == 0)
    return (true);
  else
    return (false);
};

//*************  Function for KEYS_SET class  ************
//##ModelId=3B0C085F0395
bool KEYS_SET::AddKey(string CollName, string KeyName) {
  int AttId = GetAttId(CollName, KeyName);

  // check duplicate element in vector
  for (int i = 0; i < KeyArray.size(); i++)
    if (AttId == KeyArray[i]) return false;

  // if unique
  KeyArray.push_back(AttId);

  return true;
}

bool KEYS_SET::AddKey(int AttId) {
  // check duplicate element in vector
  for (int i = 0; i < KeyArray.size(); i++)
    if (AttId == KeyArray[i]) return false;

  // if unique
  KeyArray.push_back(AttId);

  return true;
}

bool KEYS_SET::ContainKey(int AttId) {
  // check if the attid is in the vector
  for (int i = 0; i < KeyArray.size(); i++)
    if (AttId == KeyArray[i]) return true;

  return false;
}

// return the int array from the keys_set
int *KEYS_SET::CopyOut() {
  int size = GetSize();
  int *result = new int[size];
  for (int i = 0; i < size; i++) result[i] = KeyArray[i];

  return result;
}

// return the int array of size one from the keys_set
int *KEYS_SET::CopyOutOne(int i) {
  int *result = new int[1];
  result[0] = KeyArray[i];

  return result;
}

// Transform each key from A.B to NewName.B (actually the IDs)
void KEYS_SET::update(string NewName) {
  int Size = KeyArray.size();
  for (int i = 0; i < Size; i++) KeyArray[i] = GetAttId(NewName, TruncName(GetAttName(KeyArray[i])));
}

// string temp = GetAttName(KeyArray[index]);
// returns A.X temp.Format("%s");

// Returns the CuCard of the attribute
float KEYS_SET::GetAttrCuCard(int index) {
  Attribute *attr;
  attr = Cat->GetAttr(KeyArray[index]);
  return attr->CuCard;
}

// choose the attribute with Max CuCard
int KEYS_SET::ChMaxCuCard() {
  string cname1, cname2;
  float cucard1, cucard2;
  int collid;
  int win = 0;
  for (int i = 0; i < GetSize(); i++) {
    collid = GetCollId(KeyArray[i]);
    cname1 = GetCollName(collid);
    cucard1 = GetAttrCuCard(i);
    collid = GetCollId(KeyArray[win]);
    cname2 = GetCollName(collid);
    if ((cname1 == cname2)) {
      cucard2 = GetAttrCuCard(win);
      if (cucard1 >= cucard2) win = i;
    }
  }
  return win;
}

// dump KEYS_SET
// This function is not used anywhere
// but was crucial in writing another
// useful function namely
// PHYS_PROP::bestKey()
/*KEYS_SET* KEYS_SET::best()
{
        KEYS_SET* bestKeySet=new KEYS_SET();
        int win = ChMaxCuCard();
    int* result = CopyOutOne(win);
    int value = result[0];
        bestKeySet->AddKey(value);
        /****************************
        bestKeySet = this;
    int win = this->ChMaxCuCard();
    int Size = this->GetSize();
        for (int i=0; i<Size; i++)
        if (i != win)
        {
         bestKeySet->RemoveKeysSet(i);
        }

        ******************************


        return bestKeySet;



}*/

string KEYS_SET::Dump() {
  string os;
  os = "(";
  int i;
  for (i = 0; i < GetSize() - 1; i++) os += GetAttName(KeyArray[i]) + ",";

  if (GetSize())
    os += GetAttName(KeyArray[i]) + ")";
  else
    os += ")";

  return os;
}

CollectionsProperties::CollectionsProperties(CollectionsProperties &other)  // copy constructor
{
  Card = other.Card;
  Keys = other.Keys;
  CandidateKey = other.CandidateKey;
  for (int i = 0; i < other.FKeyArray.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*other.FKeyArray[i]);
    FKeyArray.push_back(fk);
  }
  Order = other.Order;
  UCard = other.UCard;
  Width = other.Width;
};

//##ModelId=3B0C0860026A
void CollectionsProperties::update(string NewName) {
  Keys->update(NewName);
  CandidateKey->update(NewName);
  for (int i = 0; i < FKeyArray.size(); i++) {
    FKeyArray[i]->update(NewName);
  }
}

// dump collection property content
string CollectionsProperties::Dump() {
  string os;
  os = "Card: " + to_string(Card) + "\tUCard: " + to_string(UCard);
  if (Keys->GetSize() > 0) os += "\tOrder: " + OrderToString(Order);

  os += "\tKeys: " + (*Keys).Dump();

  os += "\tCandidateKey: " + (*CandidateKey).Dump() + "\n";

  if (FKeyArray.size() > 0) {
    os += "  Foreign Keys:";
    for (int i = 0; i < FKeyArray.size(); i++) {
      if (i < FKeyArray.size() - 1)
        os += (*FKeyArray[i]).Dump() + "\r";
      else
        os += (*FKeyArray[i]).Dump() + "\n";
    }
  }

  return os;
}

void IndexProperties::update(string NewName) { Keys->update(NewName); }

// dump index property content
string IndexProperties::Dump() {
  return "  Type:" + IndexOrderToString(IndType) + "  Keys:" + (*Keys).Dump() +
         (Clustered == true ? "  Clustered" : "  not Clustered");
}

void BitIndexProperties::update(string NewName) { BitAttr->update(NewName); }

// dump index property content
string BitIndexProperties::Dump() {
  return "  Bit Attributes:", (*BitAttr).Dump() + "  Index Attributes:" + GetAttName(IndexAttr);
}

string FOREIGN_KEY::Dump() {
  return "(  Foreign Key:" + (*ForeignKey).Dump() + "  reference to:" + (*RefKey).Dump() + "  )";
}

Attribute::Attribute(string range_var, int *atts, int size) {
  ATTR_EXP *ae = new ATTR_EXP(range_var, CopyArray(atts, size), size);
  AttId = ae->GetAttNew()->AttId;
  CuCard = ae->GetAttNew()->CuCard;
  Min = ae->GetAttNew()->Min;
  Max = ae->GetAttNew()->Max;

  delete ae;
};  // Attribute::Attribute(string range_var, int * atts, int size)

// Attribute dump function
string Attribute::Dump() {
  return GetAttName(AttId) + ":\tDomain: " + DomainToString(Cat->GetDomain(AttId)) + "\tCuCard: " + to_string(CuCard) +
         "\tMin: " + to_string(Min) + "\tMax: " + to_string(Max);
};

string Attribute::attrDump() { return GetAttName(AttId); }

string Attribute::DumpCOVE() { return GetAttName(AttId) + ":" + to_string((int)CuCard) + "  "; };

// Schema function
bool Schema::AddAttr(int Index, Attribute *attr) {
  assert(Index < Size);
  Attrs[Index] = attr;

  return true;
}

// return true if the relname.attname is in the schema
bool Schema::InSchema(int AttId) {
  int i = 0;
  for (i = 0; i < Size; i++)
    if (AttId == Attrs[i]->AttId) break;

  if (i < Size)
    return true;
  else
    return false;
}

// max cucard of each tables in the schema
float Schema::GetTableMaxCuCard(int TableIndex) {
  float Max = 0;

  for (int i = 0; i < Size; i++) {
    int CollId = GetCollId(Attrs[i]->AttId);
    // 0 is used for attr generated by rangevar(e.g. func_op(<A.X> as sum) )
    // along the query tree, they are not from any table
    if (CollId == 0) return Max;
    if (CollId == TableId[TableIndex])  // the attr is from the table
    {
      if (Max < Attrs[i]->CuCard) Max = Attrs[i]->CuCard;
    }
  }

  return Max;
}

// width of the table in the schema
float Schema::GetTableWidth(int TableIndex) {
  // add Width=0 for Table "", used for AGG_OP
  if (TableId[TableIndex] == 0) return 0;
  return Cat->GetCollProp(TableId[TableIndex])->Width;
}

// projection
// 	projection of attrs onto schema
Schema *Schema::projection(int *attrs, int size) {
  Schema *new_schema = new Schema(size);

  new_schema->TableNum = 0;
  new_schema->TableId = new int[this->TableNum];

  // add attribute sets from left operand
  for (int i = 0; i < size; i++) {
    int index = 0;
    for (index = 0; index < this->Size; index++) {
      if (attrs[i] == this->Attrs[index]->AttId) {
        // has attr op in projection list -- add it in:
        // Attribute * Attr = new Attribute(Attrs[index]->AttId , Attrs[index]->CuCard, -1, -1);
        Attribute *Attr = new Attribute(*Attrs[index]);
        new_schema->AddAttr(i, Attr);

        // get the table info for the new schema
        int CollId = GetCollId(Attr->AttId);

        for (int i = 0; i < new_schema->TableNum; i++)
          if (CollId == new_schema->TableId[i]) break;

        if (i == new_schema->TableNum)                             // a new table in the schema
          new_schema->TableId[(new_schema->TableNum)++] = CollId;  // store the table id

        break;
      }
    }
    if (index == Size) assert(false);  // project list not in schema
  }

  return (new_schema);

}  // projection(attrs)

// union the attributes from the two joined Schema.
// also check the joined predicates(attributes) are in the catalog(schema)
// calculate the ATT_PROP
Schema *Schema::UnionSchema(Schema *other) {
  int i, j;

  // union the schemas
  int LSize = this->GetSize();
  int RSize = other->GetSize();

  Schema *schema = new Schema(LSize + RSize);

  schema->TableNum = this->TableNum + other->TableNum;
  schema->TableId = new int[schema->TableNum];
  for (i = 0; i < this->TableNum; i++) schema->TableId[i] = this->TableId[i];
  for (j = 0; j < other->TableNum; j++) schema->TableId[i + j] = other->TableId[j];

  Attribute *Attr;
  for (i = 0; i < LSize; i++) {
    // from cascade
    // we calculate new cucards, in a very very crude way.
    // New cucards are half the old ones :)
    float CuCard = (*this)[i]->CuCard;
    CuCard = (CuCard != -1) ? CuCard / 2 : -1;
    float min = (*this)[i]->Min;
    float max = (*this)[i]->Max;
    Attr = new Attribute((*this)[i]->AttId, CuCard, min, max);
    schema->AddAttr(i, Attr);
  }

  for (j = 0; j < RSize; j++) {
    // from cascade
    // we calculate new cucards, in a very very crude way.
    // New cucards are half the old ones :)
    float CuCard = (*other)[j]->CuCard;
    CuCard = (CuCard != -1) ? CuCard / 2 : -1;
    float min = (*other)[j]->Min;
    float max = (*other)[j]->Max;
    Attr = new Attribute((*other)[j]->AttId, CuCard, min, max);
    schema->AddAttr(i + j, Attr);
  }

  return schema;
}

// return true if contains all the keys
bool Schema::Contains(KEYS_SET *Keys) {
  for (int i = 0; i < Keys->GetSize(); i++) {
    if (!InSchema((*Keys)[i])) return false;
  }

  return true;

}  // Contains

// free up memory
Schema::~Schema() {
  for (int i = 0; i < Size; i++) delete Attrs[i];
  delete Attrs;

  delete[] TableId;
}

// Schema dump function
string Schema::Dump() {
  string os;
  for (int i = 0; i < Size; i++) {
    os += (*(Attrs[i])).Dump();
    os += "\n";
  }
  return os;
}

string Schema::DumpCOVE() {
  string os;
  for (int i = 0; i < Size; i++) os += (*(Attrs[i])).DumpCOVE();
  return os;
}

// Schema attributes store function
KEYS_SET *Schema::AttrStore() {
  KEYS_SET *largeKeySet = new KEYS_SET();
  // PTRACE ("Schema Size is %d", GetSize());
  for (int i = 0; i < GetSize(); i++) {
    // os += (*(Attrs[i])).attrDump();
    // PTRACE("attribute dump is %s", (*(Attrs[i])).attrDump());
    if ((IntOrdersSet.ContainKey(Attrs[i]->AttId)) == true) {
      largeKeySet->AddKey(Attrs[i]->AttId);
    }
  }
  return largeKeySet;
}

// LOG_COLL_PROP dump function
string LOG_COLL_PROP::Dump() {
  string os;
  os = "  Card: " + to_string(Card) + "  UCard: " + to_string(UCard);  // + "\nSchema:\n" + (*schema).Dump();
  if (CandidateKey->GetSize() > 0) os += "CandidateKey:" + (*CandidateKey).Dump() + "\n";

  if (FKeyList.size() > 0) {
    os += "  Foreign Keys:";
    for (int i = 0; i < FKeyList.size(); i++) {
      if (i < FKeyList.size() - 1)
        os += (*FKeyList[i]).Dump() + "\r";
      else
        os += (*FKeyList[i]).Dump() + "\n";
    }
  }
  return os;
};

// LOG_COLL_PROP dump function for COVE script
string LOG_COLL_PROP::DumpCOVE() {
  return to_string((int)Card) + " " + to_string(UCard) + " { " + (*schema).DumpCOVE() + " }";
};

// misc functions

// Get Collection id from name, using CollTable dictionary
// If not present, add it
int GetCollId(int AttId) {
  if (AttId == 0) return 0;
  assert(AttId < AttCollTable.size());
  return AttCollTable[AttId];
}

// Get the ids from names
int GetCollId(string CollName) {
  int Size = CollTable.size();
  int i = 0;
  for (i = 0; i < Size; i++)
    if (CollName == CollTable[i]) break;

  if (i == Size) CollTable.push_back(CollName);

  return i;
}

// Get Att id from name, using AttTable dictionary
// If not present, add full Att name to AttTable, entry to AttCollTable

int GetAttId(string CollName, string AttName) {
  string Name = CollName + "." + AttName;
  int Size = AttTable.size();
  int i = 0;
  for (i = 0; i < Size; i++) {
    if (Name == AttTable[i]) break;
  }

  if (i == Size)  // the entry not exist, new it
  {
    AttTable.push_back(Name);
    AttCollTable.push_back(GetCollId(CollName));
  }

  return i;
}

int GetAttId(string Name) {
  int pos = Name.find('.');
  assert(pos != -1);

  int Size = AttTable.size();
  int i = 0;
  for (int i = 0; i < Size; i++)
    if (Name == AttTable[i]) break;

  if (i == Size)  // the entry not exist, new it
  {
    AttTable.push_back(Name);
    string CollName = Name.substr(0, pos);
    AttCollTable.push_back(GetCollId(CollName));
  }

  return i;
}

// Get the ids from names
int GetIndId(string CollName, string IndName) {
  string Name = CollName + "." + IndName;
  int i = 0;
  int Size = IndTable.size();
  for (int i = 0; i < Size; i++)
    if (Name == IndTable[i]) break;

  if (i == Size)  // the entry not exist, new it
    IndTable.push_back(Name);

  return i;
}

// Get the ids from names
int GetBitIndId(string CollName, string BitIndName) {
  string Name = CollName + "." + BitIndName;
  int Size = BitIndTable.size();
  int i = 0;
  for (int i = 0; i < Size; i++)
    if (Name == BitIndTable[i]) break;

  if (i == Size)  // the entry not exist, new it
    BitIndTable.push_back(Name);

  return i;
}

// Get the names from Ids
string GetCollName(int CollId) {
  if (CollId == 0) return "";
  assert(CollId < CollTable.size());
  return CollTable[CollId];
}

string GetAttName(int AttId) {
  if (AttId == 0) return "";
  assert(AttId < AttTable.size());
  return AttTable[AttId];
}

// Transform A.B to B
string TruncName(string AttName) {
  const char *p = strstr(AttName.c_str(), ".");
  assert(p);  // Input was not of the form A.B
  p++;        // skip over .
  return p;
}

string GetIndName(int IndId) {
  if (IndId == 0) return "";
  assert(IndId < IndTable.size());
  return IndTable[IndId];
}

string GetBitIndName(int BitIndId) {
  if (BitIndId == 0) return "";
  assert(BitIndId < BitIndTable.size());
  return BitIndTable[BitIndId];
}

DOM_TYPE atoDomain(char *p) {
  if (strcmp(p, "string_t") == 0) return string_t;
  if (strcmp(p, "int_t") == 0) return int_t;
  if (strcmp(p, "real_t") == 0) return real_t;
  if (strcmp(p, "unknown") == 0) return unknown;
  OUTPUT_ERROR("Domain type");
  return string_t;
}

string DomainToString(DOM_TYPE p) {
  if (p == string_t) return "string_t";
  if (p == int_t) return "int_t";
  if (p == real_t) return "real_t";
  if (p == unknown) return "unknown";
  OUTPUT_ERROR("Domain type");
  return "";
}

ORDER_AD atoKeyOrder(char *p) {
  if (strcmp(p, "ascending") == 0) return ascending;
  if (strcmp(p, "descending") == 0) return descending;
  OUTPUT_ERROR("Key order type");
  return ascending;
}

ORDER atoCollOrder(char *p) {
  if (strcmp(p, "heap") == 0) return heap;
  if (strcmp(p, "hashed") == 0) return hashed;
  if (strcmp(p, "sorted") == 0) return sorted;
  if (strcmp(p, "any") == 0) return any;
  OUTPUT_ERROR("Coll order type");
  return heap;
}

string OrderToString(ORDER p) {
  if (p == heap) return "heap";
  if (p == hashed) return "hashed";
  if (p == sorted) return "sorted";
  OUTPUT_ERROR("Coll order type");
  return "";
}

ORDER_INDEX atoIndexOrder(char *p) {
  if (strcmp(p, "btree") == 0) return btree;
  if (strcmp(p, "hash") == 0) return ORDER_INDEX::hash;
  OUTPUT_ERROR("Index order type");
  return btree;
}

string IndexOrderToString(ORDER_INDEX p) {
  if (p == btree) return "btree";
  if (p == ORDER_INDEX::hash) return "hash";
  OUTPUT_ERROR("Index order type");
  return "";
}

// return only the file name (without path)
// used by TRACE function
string Trim(string PathName) { return PathName; };

// skip the blank space
char *SkipSpace(char *p) {
  while (*p == ' ' || *p == '\t') p++;
  return p;
}

// return true if the string line is Comment or blank line
bool IsCommentOrBlankLine(char *p) {
  p = SkipSpace(p);

  if (*p == '\n' || *p == 0) return true;  // blank line

  if (*p == '/' && *(p + 1) == '/')
    return true;  // comment line
  else
    return false;
}

void parseString(char *p) {
  p = SkipSpace(p);
  while (*p != ' ' && *p != '\t' && *p != '\n' && *p != '\r') p++;  // keep the char until blank space
  *p = 0;
}

//=============  PHYS_PROP Methods  ===================
PHYS_PROP::PHYS_PROP(KEYS_SET *Keys, ORDER Order) : Keys(Keys), Order(Order){};

// a constructor for ANY property
PHYS_PROP::PHYS_PROP(ORDER Order) : Keys(NULL), Order(Order) { assert(Order == any); }

PHYS_PROP::PHYS_PROP(PHYS_PROP &other)
    : Keys(other.Order == any ? NULL : new KEYS_SET(*(other.Keys))), Order(other.Order) {
  if (Order == sorted) {
    assert(other.KeyOrder.size() == other.Keys->GetSize());
    for (int i = 0; i < other.KeyOrder.size(); i++) this->KeyOrder.push_back(other.KeyOrder[i]);
  }
}

void PHYS_PROP::Merge(PHYS_PROP &other) {
  assert(Order == other.Order);  // only idential orders can be merge

  Keys->Merge(*(other.Keys));
  if (Order == sorted) {
    for (int i = 0; i < other.KeyOrder.size(); i++) this->KeyOrder.push_back(other.KeyOrder[i]);
  }
}

bool PHYS_PROP::operator==(PHYS_PROP &other) {
  // Note that operator== is defined properly for enums,
  // by default
  if (other.Order == any && Order == any) return true;

  // if one is any and the other is not any
  if (other.Order == any || Order == any) return false;

  if (other.Order == Order && *(other.Keys) == *Keys) {
    if (Order == sorted) {
      if (KeyOrder.size() != other.KeyOrder.size()) return false;

      for (int i = 0; i < KeyOrder.size(); i++)
        if (KeyOrder[i] != other.KeyOrder[i]) return false;
    }
    return true;
  }
  return false;
}  // PHYS_PROP::operator==

string PHYS_PROP::Dump() {
  string os;

  if (Order == any)
    os = "Any Prop";
  else {
    string s;
    os = (Order == heap ? s = "heap"
                        : (Order == sorted ? s = "sorted" : (Order == hashed ? s = "hashed" : s = "UNKNOWN"))) +
         " on " + Keys->Dump();
  }
  if (Order == sorted) {
    os += "  KeyOrder: (";
    int i;
    string s;
    for (i = 0; i < KeyOrder.size() - 1; i++) {
      os += " " + (KeyOrder[i] == ascending ? s = "ascending" : s = "descending");
    }
    os += KeyOrder[i] == ascending ? "ascending" : "descending";
  }

  return os;
}

void PHYS_PROP::bestKey() {
  // find the index of the KeyArray
  // with maximum unique cardinality
  int win = Keys->ChMaxCuCard();
  int Size = Keys->GetSize();
  int SizeKeyOrder = KeyOrder.size();

  // make sure that both the CArrays ,
  // Keys and KeyOrder are equal in size
  //  and also that win is within the size
  // of these CArrays
  assert(Size == SizeKeyOrder);
  assert(win <= Size);

  // copy out in an array just the element at
  // position win and store its value in KeyValue
  int *result = Keys->CopyOutOne(win);
  int KeyValue = result[0];

  // Set the size of the original Keys
  // to 0 and then fill the above KeyValue in it
  Keys->reset();
  Keys->AddKey(KeyValue);

  KeyOrder[0] = KeyOrder[win];
  KeyOrder.resize(1);
  delete result;
}

//=============  CONT Methods  ===================

CONT::CONT(PHYS_PROP *RP, Cost *U, bool D) : ReqdPhys(RP), UpperBd(U), Finished(false) {
  // If the Physical Property has >1 attribute, use only the most selective attribute
  if (RP && (RP->GetKeysSet()) && (RP->GetKeysSet()->GetSize() > 1))
    // RP-> SetKeysSet( RP->GetKeysSet() -> best());
    RP->bestKey();
};

vector<CONT *> CONT::vc;

//=============  Cost Methods  ===================

void Cost::FinalCost(Cost *LocalCost, Cost **TotalInputCost, int Size) {
  *this = *LocalCost;

  for (int i = Size; --i >= 0;) {
    assert(TotalInputCost[i]);
    *this += *(TotalInputCost[i]);
  }
  return;
}

string Cost::Dump() { return to_string(Value); }