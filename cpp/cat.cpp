
#include "../header/cat.h"

#include "../header/stdafx.h"

#define LINEWIDTH 256  // buffer length of one text line

// Keyword definition
#define KEYWORD_COLLNAME "RelName:"
#define KEYWORD_CARD "Card:"
#define KEYWORD_UCARD "UCard:"
#define KEYWORD_WIDTH "Width:"
#define KEYWORD_ORDER "Order:"
#define KEYWORD_ORDERKEY "OrderKey:"
#define KEYWORD_NUMOFATTR "NumOfAttr:"
#define KEYWORD_NUMOFINDEX "NumofIndex:"
#define KEYWORD_CANDIDATEKEY "CandidateKey:"
#define KEYWORD_NUMOFBITINDEX "NumofBitIndex:"
#define KEYWORD_NUMOFFOREIGNKEY "NumofForeignKey:"
#define KEYWORD_KEYORDER "KeyOrder:"

// read catalog text file and store the information into CAT
CAT::CAT(string filename) {
  FILE *fp;                  // file handle
  char TextLine[LINEWIDTH];  // text line buffer
  char *p;

  CollectionsProperties *CollProp;
  Attribute *attribute;
  IndexProperties *Index;
  BitIndexProperties *BitIndexProp;

  string CollName;
  string AttrName;
  string IndexName;
  string BitIndexName;

  // initialize the global tables with trivial entries
  CollTable.resize(0);
  AttTable.resize(0);
  AttCollTable.resize(0);
  IndTable.resize(0);
  BitIndTable.resize(0);
  CollTable.push_back("");
  AttTable.push_back("");
  AttCollTable.push_back(0);
  IndTable.push_back("");
  BitIndTable.push_back("");  // 632

  if ((fp = fopen(filename.c_str(), "r")) == NULL) OUTPUT_ERROR("can not open file 'catalog'");

  bool FirstTime = true;
  for (;;) {
    fgets(TextLine, LINEWIDTH, fp);
    if (feof(fp)) break;

    // skip the comment line
    if (IsCommentOrBlankLine(TextLine)) continue;

    p = SkipSpace(TextLine);
    // get relname
    if (p == strstr(p, KEYWORD_COLLNAME)) {
      if (!FirstTime) {
        // encounter a new relation definition,
        // meaning the last one had finished
        // Add to CollProp map
        AddColl(CollName, CollProp);
      }

      CollProp = new CollectionsProperties;

      p += strlen(KEYWORD_COLLNAME);
      // Add to BULK_PROP map

      p = SkipSpace(p);
      parseString(p);

      CollName = p;

      // set the foreignkeystring size to 0
      CollProp->ForeignKeyString.resize(0);

      FirstTime = false;  // now had got a relname

      continue;
    }

    // get UCARD
    if (p == strstr(p, KEYWORD_UCARD)) {
      p += strlen(KEYWORD_UCARD);
      p = SkipSpace(p);
      parseString(p);
      CollProp->UCard = (float)atof(p);
      continue;
    }

    // get CARD
    if (p == strstr(p, KEYWORD_CARD)) {
      p += strlen(KEYWORD_CARD);
      p = SkipSpace(p);
      parseString(p);
      CollProp->Card = (float)atof(p);
      continue;
    }

    // get WIDTH
    if (p == strstr(p, KEYWORD_WIDTH)) {
      p += strlen(KEYWORD_WIDTH);
      p = SkipSpace(p);
      parseString(p);
      CollProp->Width = (float)atof(p);
      continue;
    }

    // get ORDERKEY
    if (p == strstr(p, KEYWORD_ORDERKEY)) {
      p += strlen(KEYWORD_ORDERKEY);
      KEYS_SET *Keys = new KEYS_SET;
      parseKeys(p, Keys, CollName);
      CollProp->Keys = Keys;
      continue;
    }

    // get ORDER
    if (p == strstr(p, KEYWORD_ORDER)) {
      p += strlen(KEYWORD_ORDER);
      p = SkipSpace(p);
      parseString(p);
      CollProp->Order = atoCollOrder(p);
      continue;
    }

    // get KEYORDER
    if (p == strstr(p, KEYWORD_KEYORDER)) {
      p += strlen(KEYWORD_KEYORDER);
      char *str;

      while (*p != '(') p++;  // skip the char before '('
      if (*p != '(') OUTPUT_ERROR("can't find '(' when parse KEYS_ORDER!");
      p++;

      while (*p != ')') {
        str = p;
        p = SkipSpace(p);
        while (*p != ' ' && *p != '\t' && *p != ',' && *p != ')' && *p != '\n') p++;  // keep the char
        if (*p == ')') {
          *p = 0;  // get a str
          str = SkipSpace(str);
          CollProp->KeyOrder.push_back(atoKeyOrder(str));  // add the order to KeyOrder
          break;
        }
        if (*p == ',') {
          *p = 0;                                          // get a str
          CollProp->KeyOrder.push_back(atoKeyOrder(str));  // add the order to KeyOrder
          *p = ',';                                        // restore the char
          p++;
          continue;
        }
        // else
        OUTPUT_ERROR("format error when parse KEYS_ORDER!");
      }
      continue;
    }

    // get CANDIDATEKEY
    if (p == strstr(p, KEYWORD_CANDIDATEKEY)) {
      p += strlen(KEYWORD_CANDIDATEKEY);
      KEYS_SET *Keys = new KEYS_SET;
      parseKeys(p, Keys, CollName);
      CollProp->CandidateKey = Keys;
      continue;
    }  // end of get CANDIDATEKEY

    // begin parse Attribute Set
    if (p == strstr(p, KEYWORD_NUMOFATTR)) {
      p += strlen(KEYWORD_NUMOFATTR);
      p = SkipSpace(p);
      parseString(p);

      int NumberOfAttribute = atoi(p);
      for (int i = 0; i < NumberOfAttribute;) {
        fgets(TextLine, LINEWIDTH, fp);

        if (feof(fp)) OUTPUT_ERROR("EOF when reading attribute!");

        if (IsCommentOrBlankLine(TextLine)) continue;

        attribute = new Attribute;
        DOM_TYPE domain;
        parseAttribute(TextLine, AttrName, attribute, domain);

        i++;

        // add to catalog
        AddAttr(CollName, AttrName, attribute, domain);
      }
      continue;
    }

    // begin parse Index Set
    if (p == strstr(p, KEYWORD_NUMOFINDEX)) {
      p += strlen(KEYWORD_NUMOFINDEX);
      p = SkipSpace(p);
      parseString(p);

      int NumberOfIndex = atoi(p);
      for (int i = 0; i < NumberOfIndex;) {
        fgets(TextLine, LINEWIDTH, fp);

        if (feof(fp)) OUTPUT_ERROR("EOF when reading Index info!");

        if (IsCommentOrBlankLine(TextLine)) continue;

        Index = new IndexProperties;
        parseIndex(TextLine, CollName, IndexName, Index);
        i++;

        // add to ITEM_PROP map
        AddIndex(CollName, IndexName, Index);
      }
      continue;
    }  // end of parsing index

    // begin parse BitIndex Set
    if (p == strstr(p, KEYWORD_NUMOFBITINDEX)) {
      p += strlen(KEYWORD_NUMOFBITINDEX);
      p = SkipSpace(p);
      parseString(p);

      int NumberOfBitIndex = atoi(p);
      for (int i = 0; i < NumberOfBitIndex;) {
        fgets(TextLine, LINEWIDTH, fp);

        if (feof(fp)) OUTPUT_ERROR("EOF when reading BitIndex info!");

        if (IsCommentOrBlankLine(TextLine)) continue;

        BitIndexProp = new BitIndexProperties;
        parseBitIndex(TextLine, CollName, BitIndexName, BitIndexProp);
        i++;

        // add to BITIND_PROP map
        AddBitIndex(CollName, BitIndexName, BitIndexProp);
      }
      continue;
    }  // end of parsing index

    // begin parse foreign key
    if (p == strstr(p, KEYWORD_NUMOFFOREIGNKEY)) {
      p += strlen(KEYWORD_NUMOFFOREIGNKEY);
      p = SkipSpace(p);
      parseString(p);

      int NumberOfFKey = atoi(p);
      for (int i = 0; i < NumberOfFKey;) {
        fgets(TextLine, LINEWIDTH, fp);

        if (feof(fp)) OUTPUT_ERROR("EOF when reading Foreign Key info!");

        if (IsCommentOrBlankLine(TextLine)) continue;
        p = SkipSpace(TextLine);
        // just store this string for now, the string
        // will be translated to foreignkeys at the end of CAT
        // otherwise the attr referenced may not be in the catalog
        CollProp->ForeignKeyString.push_back(p);
        i++;
      }
      continue;
    }  // end of parsing foreign key

  }  // end of parsing catalog loop

  // the last time to add relname
  AddColl(CollName, CollProp);
  fclose(fp);

  // translate foreign key string to foreignkey
  for (int CollId = 1; CollId < CollProps.size(); CollId++) {
    CollName = CollTable[CollId];
    CollProp = CollProps[CollId];
    if (CollProp->ForeignKeyString.size() > 0) {
      for (int keyNum = 0; keyNum < CollProp->ForeignKeyString.size(); keyNum++) {
        p = CollProp->ForeignKeyString[keyNum].data();
        KEYS_SET *FKeys = new KEYS_SET;
        KEYS_SET *RKeys = new KEYS_SET;
        parseKeys(p, FKeys, CollName);
        p += strlen(p) + 1;
        GetKey(p, RKeys);
        CollProp->FKeyArray.push_back(new FOREIGN_KEY(FKeys, RKeys));
        // CollProp->ForeignKeyString[keyNum].ReleaseBuffer();
      }
    }
  }

  // translate BitAttrString to AttId
  for (int BitIndID = 1; BitIndID < BitIndProps.size(); BitIndID++) {
    BitIndProps[BitIndID]->IndexAttr = GetAttId(BitIndProps[BitIndID]->IndexAttrString);
  }

  // check KeyOrder.size() == Keys.size()
  if (CollProp->Order == sorted)
    if (CollProp->KeyOrder.size() != CollProp->Keys->GetSize())
      OUTPUT_ERROR("KeyOrder size and Keys size is not equal!");
}

// free memory of catalog structure
CAT::~CAT() {
  int i;

  // free collection properties
  for (i = 1; i < CollProps.size(); i++) delete CollProps[i];

  // free attributes
  for (i = 1; i < Attrs.size(); i++) delete Attrs[i];

  // free index properties
  for (i = 1; i < IndProps.size(); i++) delete IndProps[i];

  // free bit index properties
  for (i = 1; i < BitIndProps.size(); i++) delete BitIndProps[i];

  // free attribute names table
  for (i = 1; i < AttNames.size(); i++) delete AttNames[i];

  // free index names table
  for (i = 1; i < IndNames.size(); i++) delete IndNames[i];

  // free bitindex names table
  for (i = 1; i < BitIndNames.size(); i++) delete BitIndNames[i];
  CollTable.clear();
  AttTable.clear();
  IndTable.clear();
  AttCollTable.clear();
  BitIndTable.clear();
}

// Add CollProp for this collection.  If Collection is new, also update CollTable
void CAT::AddColl(string CollName, CollectionsProperties *CollProp) {
  int CollId = GetCollId(CollName);
  if (CollId >= CollProps.size()) CollProps.resize(CollId + 1);
  CollProps[CollId] = CollProp;
}

// fill tables related to attributes
// If Attribute or Collection are new, add them to AttProps, AttTable, AttNames, resp.
// Add Attr to Attrs table, Attribute to Attnames
void CAT::AddAttr(string CollName, string AttName, Attribute *Attr, DOM_TYPE domain) {
  // If Attribute is new, add it to AttProps.  Add AttProp to AttProps table.
  int AttId = GetAttId(CollName, AttName);
  if (AttId >= Attrs.size()) Attrs.resize(AttId + 1);
  Attr->AttId = AttId;
  Attrs[AttId] = Attr;
  if (AttId >= Domains.size()) Domains.resize(AttId + 1);
  Domains[AttId] = domain;

  // If Collection is new, add it to AttNames.  Add Attribute to AttNames
  int CollId = GetCollId(CollName);
  if (CollId >= AttNames.size())  // if the entry not exist, new it
  {
    AttNames.resize(CollId + 1);
    AttNames[CollId] = new INT_ARRAY;
  }

  AttNames[CollId]->push_back(AttId);
}

// If Index, COllection are new, add them to IndProps, IndNames, respectively.
// Add IndProp, Index to IndProps, IndNames, resp.
void CAT::AddIndex(string CollName, string IndexName, IndexProperties *IndProp) {
  // If Index is new, add it to IndProps.  Add IndProp to IndProps
  int IndId = GetIndId(CollName, IndexName);
  if (IndId >= IndProps.size()) IndProps.resize(IndId + 1);
  IndProps[IndId] = IndProp;

  // If Index is new, add it to IndProps.  Add IndProp to IndProps
  int CollId = GetCollId(CollName);
  if (CollId >= IndNames.size())  // if the entry not exist, new it
  {
    IndNames.resize(CollId + 1);
    IndNames[CollId] = new INT_ARRAY;
  }

  IndNames[CollId]->push_back(IndId);
}

// If BitIndex, COllection are new, add them to BitIndProps, BitIndNames, respectively.
// Add BitIndProp, BitIndex to BitIndProps, BitIndNames, resp.
//##ModelId=3B0C0878008E
void CAT::AddBitIndex(string CollName, string BitIndexName, BitIndexProperties *BitIndProp) {
  // If BitIndex is new, add it to BitIndProps.  Add BitIndProp to BitIndProps
  int BitIndId = GetBitIndId(CollName, BitIndexName);
  if (BitIndId >= BitIndProps.size()) BitIndProps.resize(BitIndId + 1);
  BitIndProps[BitIndId] = BitIndProp;

  // If Index is new, add it to IndProps.  Add IndProp to IndProps
  int CollId = GetCollId(CollName);
  if (CollId >= BitIndNames.size())  // if the entry not exist, new it
  {
    BitIndNames.resize(CollId + 1);
    BitIndNames[CollId] = new INT_ARRAY;
  }

  BitIndNames[CollId]->push_back(BitIndId);
}

// get one KeySET, and add to KEYS_SET
//##ModelId=3B0C0878016A
void CAT::GetKey(char *p, KEYS_SET *Keys) {
  while (*p != '(') p++;  // skip the char before '('
  if (*p != '(') OUTPUT_ERROR("can't find '(' when parse KEYS_SET!");

  p++;  // skip '('

  while (*p != ')') {
    string str1, str2;
    // str1 = p;
    p = SkipSpace(p);
    while (*p != ' ' && *p != '\t' && *p != ',' && *p != ')' && *p != '\n' && *p != '.') str1 += *p++;  // keep the char
    if (*p == '.') {
      p++;
      while (*p != ' ' && *p != '\t' && *p != ',' && *p != ')' && *p != '\n' && *p != '.') str2 += *p++;
      if (*p == ')') {
        Keys->AddKey(str1, str2);  // add the string to the collection
        return;
      }
      if (*p == ',') {
        Keys->AddKey(str1, str2);  // add the string to the collection
        p++;
        continue;
      }
      // else
      OUTPUT_ERROR("format error when parse KEYS_SET! suppose: (X.X) or (X.X,Y.Y)");
    }
    OUTPUT_ERROR("wrong KEYS_SET format, missing DOT. ");
  }
}

// get collection property by collection Id
//##ModelId=3B0C08770385
CollectionsProperties *CAT::GetCollProp(int CollId) {
  if (CollId < CollProps.size())
    return CollProps[CollId];
  else
    return NULL;
}

// get attribute property by attribute Id
//##ModelId=3B0C0877038F
Attribute *CAT::GetAttr(int AttId) {
  if (AttId < Attrs.size())
    return Attrs[AttId];
  else
    return NULL;
}

// get attribute property by attribute Id
DOM_TYPE CAT::GetDomain(int AttId) {
  if (AttId < Domains.size())
    return Domains[AttId];
  else
    return unknown;  // this attribute is not in catalog
}

// get index property by index Id
IndexProperties *CAT::GetIndProp(int IndId) {
  if (IndId < IndProps.size())
    return IndProps[IndId];
  else
    return NULL;
}

// get Bitindex property by Bitindex Id
//##ModelId=3B0C0878000C
BitIndexProperties *CAT::GetBitIndProp(int BitIndId) {
  if (BitIndId < BitIndProps.size())
    return BitIndProps[BitIndId];
  else
    return NULL;
}

// get all the names of indexes on the collection
//##ModelId=3B0C087703C1
INT_ARRAY *CAT::GetIndNames(int CollId) {
  if (CollId < IndNames.size())
    return IndNames[CollId];
  else
    return NULL;
}

// get all the bitindex's names of the collection
//##ModelId=3B0C087703E0
INT_ARRAY *CAT::GetBitIndNames(int CollId) {
  if (CollId < BitIndNames.size())
    return BitIndNames[CollId];
  else
    return NULL;
}

// get all the attr's names of the collection
//##ModelId=3B0C087703B7
INT_ARRAY *CAT::GetAttNames(int CollId) {
  if (CollId < AttNames.size())
    return AttNames[CollId];
  else
    return NULL;
}

/*****************   parse Function  **************/
void CAT::parseKeys(char *p, KEYS_SET *Keys, string CollName) {
  char *str;

  while (*p != '(') p++;  // skip the char before '('
  if (*p != '(') OUTPUT_ERROR("can't find '(' when parse KEYS_SET!");

  p++;

  while (*p != ')') {
    str = p;
    p = SkipSpace(p);
    while (*p != ' ' && *p != '\t' && *p != ',' && *p != ')' && *p != '\n') p++;  // keep the char
    if (*p == ')') {
      *p = 0;  // get a str
      str = SkipSpace(str);
      Keys->AddKey(CollName, str);  // add the string to the collection
      return;
    }
    if (*p == ',') {
      *p = 0;                       // get a str
      Keys->AddKey(CollName, str);  // add the string to the collection
      *p = ',';                     // restore the char
      p++;
      continue;
    }
    // else
    OUTPUT_ERROR("format error when parse KEYS_SET! suppose: (X) or (X,Y)");
  }
}

// fill out attribute, except for AttId, which is filled out when adding attribute
//##ModelId=3B0C087800DE
void CAT::parseAttribute(char *p, string &AttrName, Attribute *Attribute, DOM_TYPE &domain) {
  p = SkipSpace(p);
  parseString(p);  // get the Name
  AttrName = p;
  p += strlen(p) + 1;

  p = SkipSpace(p);
  parseString(p);  // get the Domain
  // Attribute->Domain = atoDomain(p);
  domain = atoDomain(p);
  p += strlen(p) + 1;

  p = SkipSpace(p);
  parseString(p);  // get the CuCard
  Attribute->CuCard = (float)atof(p);
  p += strlen(p) + 1;

  if (domain == string_t)  // string_t has no min and max
  {
    Attribute->Max = (float)(-1);
    Attribute->Min = (float)(-1);
    return;
  }

  p = SkipSpace(p);
  parseString(p);  // get the Max
  Attribute->Max = (float)atof(p);
  p += strlen(p) + 1;

  p = SkipSpace(p);
  parseString(p);  // get the Min
  Attribute->Min = (float)atof(p);
}

void CAT::parseIndex(char *p, string CollName, string &IndexName, IndexProperties *Index) {
  p = SkipSpace(p);
  parseString(p);  // get the Name
  IndexName = p;
  p += strlen(p) + 1;

  Index->Keys = new KEYS_SET;
  p = SkipSpace(p);
  parseKeys(p, Index->Keys, CollName);  // get the Keys
  p += strlen(p) + 1;

  p = SkipSpace(p);
  parseString(p);  // get the Key type
  Index->IndType = atoIndexOrder(p);
  p += strlen(p) + 1;

  p = SkipSpace(p);  // get Clustered
  parseString(p);
  Index->Clustered = ((strcmp(p, "T") == 0) ? true : false);
}

void CAT::parseBitIndex(char *p, string CollName, string &BitIndexName, BitIndexProperties *BitIndex) {
  p = SkipSpace(p);
  parseString(p);  // get the Name
  BitIndexName = p;
  p += strlen(p) + 1;

  BitIndex->BitAttr = new KEYS_SET;
  p = SkipSpace(p);
  parseKeys(p, BitIndex->BitAttr, CollName);  // get the bit attributes
  p += strlen(p) + 1;

  p = SkipSpace(p);
  parseString(p);
  // BitIndex->IndexAttr = GetAttId(p);			//get index attribute
  BitIndex->IndexAttrString = p;
  p += strlen(p) + 1;

  // parse predicate index
  p = SkipSpace(p);
  char str[256];
  while (*p != '(') p++;  // skip the char before '('
  if (*p != '(') OUTPUT_ERROR("can't find '(' when parse bit index predicate!");
  int LeftNum = 0;
  int i = 0;
  while (*p) {
    while (*p != ' ' && *p != '\t' && *p != '\n') {
      str[i] = *p;
      if (*p == '(') LeftNum++;
      if (*p == ')') LeftNum--;
      p++;
      i++;
    }
    p++;
  }

  if (LeftNum != 0) OUTPUT_ERROR("last right_bracket not found or mismatch!");
  BitIndex->BitPredString = str;  // get predicate string
}

// dump catalog content
string CAT::Dump() {
  string os;
  string temp;

  // dump collection properties
  os += "******* CollectionsProperties: ********\n";
  for (int CollId = 1; CollId < CollProps.size(); CollId++)
    os += GetCollName(CollId) + ":\t" + CollProps[CollId]->Dump();

  // dump attribute properties
  os += "\n******** ATT_PROP: ********\n";
  for (int AttId = 1; AttId < Attrs.size(); AttId++) os += Attrs[AttId]->Dump() + "\n";

  // dump index properties
  os += "\n******** IndexProperties: ********\n";
  for (int IndId = 1; IndId < IndProps.size(); IndId++) os += GetIndName(IndId) + ":\n", IndProps[IndId]->Dump() + "\n";

  // dump bit index properties
  os += "\n******** BitIndexProperties: ********\n";
  for (int BitIndId = 1; BitIndId < BitIndProps.size(); BitIndId++)
    os += GetBitIndName(BitIndId) + ":\n" + BitIndProps[BitIndId]->Dump() + "\n";

  return os;
}
