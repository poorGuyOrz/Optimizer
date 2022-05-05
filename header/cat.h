#pragma once

#include "../header/stdafx.h"

class CAT;

// CATALOG - class CAT

class CAT {
 private:                              // Each array maps an integer into the elements of the array at that integer
                                       // location, i.e. maps i to array[i].
  vector<COLL_PROP *> CollProps;       // Array of collection properties
  vector<ATTR *> Attrs;                // Array of attributes , index is AttId
  vector<DOM_TYPE> Domains;            // Array of domains, index is AttId
  vector<INT_ARRAY *> AttNames;        // Attribute Names
  vector<IND_PROP *> IndProps;         // Properties of Indexes
  vector<INT_ARRAY *> IndNames;        // Index Names
  vector<BIT_IND_PROP *> BitIndProps;  // Properties of BitIndexes
  vector<INT_ARRAY *> BitIndNames;     // BitIndex Names

 public:
  CAT(string filename);  // read information into catalog from some default file
  ~CAT();                // free the memory of the catalog structure

  // Each of the following functions retrieves data from one of the private arrays above.
  // If the index input is not within range it returns NULL.
  COLL_PROP *GetCollProp(int CollId);
  ATTR *GetAttr(int AttId);
  DOM_TYPE GetDomain(int AttId);
  INT_ARRAY *GetAttNames(int CollId);
  INT_ARRAY *GetIndNames(int CollId);
  IND_PROP *GetIndProp(int IndId);
  INT_ARRAY *GetBitIndNames(int CollId);
  BIT_IND_PROP *GetBitIndProp(int BitIndId);

  // Each of the following functions adds data to the relevant table
  // They need to be public so we can add aliases (FROM emp AS e)

  // Add CollProp for this collection.  If Collection is new, also update CollTable
  void AddColl(string CollName, COLL_PROP *CollProp);

  // fill tables related to attributes
  // If Attribute or Collection are new, add them to AttProps, AttTable, AttNames, resp.
  // Add AttProp to AttProps table, Attribute to Attnames
  void AddAttr(string CollName, string AttrName, ATTR *attr, DOM_TYPE domain);

  // If Index, COllection are new, add them to IndProps, IndNames, respectively.
  // Add IndProp, Index to IndProps, IndNames, resp.
  void AddIndex(string CollName, string IndexName, IND_PROP *indexprop);

  // If Index, COllection are new, add them to IndProps, IndNames, respectively.
  // Add IndProp, Index to IndProps, IndNames, resp.
  void AddBitIndex(string RelName, string BitIndexName, BIT_IND_PROP *bitindexprop);

  // dump CAT content to a string
  string Dump();

 private:  // functions for parsing the catalog input file
  // get Keys from line buf. format: (xxx,xxx)
  void parseKeys(char *p, KEYS_SET *Keys, string RelName);

  // get attribute from line buf, fill tables related to attributes
  void parseAttribute(char *p, string &AttrName, ATTR *Attribute, DOM_TYPE &domain);

  // get index prop. from line buf
  void parseIndex(char *p, string RelName, string &IndexName, IND_PROP *Index);

  // get bit index prop. from line buf
  void parseBitIndex(char *p, string RelName, string &BitIndexName, BIT_IND_PROP *BitIndex);
  // get keys from line buf. format: (X.xx, X.xx)
  void GetKey(char *p, KEYS_SET *Keys);
};
