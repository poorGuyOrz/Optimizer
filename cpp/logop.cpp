// logop.cpp -  implementation of classes of logical operators

#include "../header/logop.h"

#include "../header/cat.h"
#include "../header/item.h"
#include "../header/stdafx.h"

/*********** GET functions ****************/
//##ModelId=3B0C087301FB
GET::GET(int collId) : CollId(collId) {
  name = GetName() + GetCollName(CollId);  // for debug
};

GET::GET(string collection, string rangeVar) {
  RangeVar = rangeVar;
  if (collection == rangeVar)
    CollId = GetCollId(collection);
  else {
    // It's a nontrivial range variable.  push_back new entries to all
    // relevant tables, with name of RangeVar and properties equal to those
    // of the collection.
    int collectionID = GetCollId(collection);

    // Set GET's CollId to RangeVar's new ID.
    CollId = GetCollId(rangeVar);

    // Get all atts for this collection, then add to att tables
    INT_ARRAY *AttArray = Cat->GetAttNames(collectionID);
    int Size = AttArray->size();
    Attribute *attr;
    for (int i = 0; i < Size; i++)  // For each attribute
    {
      attr = new Attribute(*(Cat->GetAttr(AttArray->at(i))));
      DOM_TYPE domain = Cat->GetDomain(AttArray->at(i));
      Cat->AddAttr(RangeVar, TruncName(GetAttName(AttArray->at(i))), attr, domain);
    }

    PTRACE("Catalog content after fixing AttId-based tables for range " << RangeVar << ":\n" << Cat->Dump());
    // OutputFile.Flush();

    // Get all indexes and ditto
    INT_ARRAY *IndArray = Cat->GetIndNames(collectionID);
    if (IndArray) {
      Size = IndArray->size();
      for (int i = 0; i < Size; i++)  // For each index
      {
        IndexProperties *indprop = new IndexProperties;
        IndexProperties *ip = (Cat->GetIndProp(IndArray->at(i)));
        *indprop = *ip;
        // Alter keys in the property object so they will refer to new range variable attributes
        indprop->update(RangeVar);
        Cat->AddIndex(RangeVar, TruncName(GetIndName(IndArray->at(i))), indprop);
      }
    }
    PTRACE("Catalog content after fixing IndId-based tables for range " << RangeVar << ":\n" << Cat->Dump());

    // Get all bit indexes and ditto
    INT_ARRAY *BitIndArray = Cat->GetBitIndNames(collectionID);
    if (BitIndArray) {
      Size = BitIndArray->size();
      for (int i = 0; i < Size; i++)  // For each index
      {
        BitIndexProperties *bitindprop = new BitIndexProperties;
        BitIndexProperties *ip = (Cat->GetBitIndProp(BitIndArray->at(i)));
        *bitindprop = *ip;
        // Alter keys in the property object so they will refer to new range variable attributes
        bitindprop->update(RangeVar);
        Cat->AddBitIndex(RangeVar, TruncName(GetBitIndName(BitIndArray->at(i))), bitindprop);
      }
    }
    PTRACE("Catalog content after fixing BitIndId-based tables for range " << RangeVar << ":\n" << Cat->Dump());

    // Populate all relevant CollId-based tables
    // Should use a cinstructor here but it's already in use.
    CollectionsProperties *collp = new CollectionsProperties;
    *collp = *(Cat->GetCollProp(collectionID));  // Will be in catalog
    collp->update(RangeVar);
    Cat->AddColl(RangeVar, collp);
    PTRACE("Catalog content after fixing CollId-based tables:" << endl << Cat->Dump());
  }

  name = GetName() + GetCollName(CollId);  // for debug
}

GET::GET(GET &Op) : CollId(Op.GetCollection()) {
  name = Op.name;  // for debug
}

string GET::Dump() { return GetName() + "(" + GetCollName(CollId) + ")"; }

// find the logical property of the collection,
// also check the schema
LOG_PROP *GET::FindLogProp(LOG_PROP **input) {
  CollectionsProperties *CollProp = Cat->GetCollProp(CollId);
  assert(CollProp != NULL);

  INT_ARRAY *AttrNames = Cat->GetAttNames(CollId);
  assert(AttrNames != NULL);

  int Size = AttrNames->size();
  assert(Size > 0);

  Schema *schema = new Schema(Size);
  schema->TableNum = 1;
  schema->TableId = new int;
  schema->TableId[0] = CollId;

  Attribute *Attr;
  int i;
  for (i = 0; i < Size; i++) {
    Attr = new Attribute(*Cat->GetAttr((*AttrNames)[i]));
    assert(Attr != NULL);
    // Attr = new Attribute((*AttrNames)[i], AttProp->CuCard);
    schema->AddAttr(i, Attr);
  }

  KEYS_SET *cand_key;
  int cand_key_size = CollProp->CandidateKey->GetSize();
  int *temp_key = CollProp->CandidateKey->CopyOut();
  cand_key = new KEYS_SET(temp_key, cand_key_size);
  delete[] temp_key;

  LOG_COLL_PROP *result = new LOG_COLL_PROP(CollProp->Card, CollProp->UCard, schema, cand_key);
  // copy the foreign key info from catalog
  for (i = 0; i < CollProp->FKeyArray.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*CollProp->FKeyArray[i]);
    result->FKeyList.push_back(fk);
  }

  return result;
}

ub4 GET::hash() {
  ub4 hashval = GetInitval();
  hashval = lookup2(CollId, hashval);
  return (hashval % (HtblSize - 1));
}

/*********** EQJOIN functions ****************/
EQJOIN::EQJOIN(int *lattrs, int *rattrs, int size) : lattrs(lattrs), rattrs(rattrs), size(size) {
  name = GetName();  // for debug
};

EQJOIN::EQJOIN(EQJOIN &Op)
    : lattrs(CopyArray(Op.lattrs, Op.size)), rattrs(CopyArray(Op.rattrs, Op.size)), size(Op.size) {
  name = Op.name;  // for debug
};

string EQJOIN::Dump() {
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
}

LOG_PROP *EQJOIN::FindLogProp(LOG_PROP **input) {
  LOG_COLL_PROP *Left = (LOG_COLL_PROP *)(input[0]);
  LOG_COLL_PROP *Right = (LOG_COLL_PROP *)(input[1]);

  assert(Left->Card >= 0);
  assert(Right->Card >= 0);
  assert(Left->UCard >= 0);
  assert(Right->UCard >= 0);

  // check the joined predicates(attributes) are in the schema
  int i;
  for (i = 0; i < size; i++) assert(Left->schema->InSchema(lattrs[i]));

  for (i = 0; i < size; i++) assert(Right->schema->InSchema(rattrs[i]));

  // union schema
  Schema *schema = Left->schema->UnionSchema(Right->schema);

  // compute join log_prop
  int ConditionNum = size;

  bool LeftFK = false;  // Is FK on left or right
  bool RightFK = false;
  bool IsFKJoin = false;    // Is this a FK join?
  bool IsFKSchema = false;  // Is it like an FK join except refkey is not a cand key of entire join
  float RefUcard = 1;       // Unique card of refkey, needed to compute eqjoin card

  // Eqjoin card is card of ref input (as opposed to FK input) divided by RefUcard

  // check if lattr contains foreign key
  for (i = 0; i < Left->FKeyList.size(); i++) {
    // if lattr contains FK
    if (Left->FKeyList[i]->ForeignKey->IsSubSet(lattrs, size)) {
      // eqjoin on the refkey
      if (Left->FKeyList[i]->RefKey->IsSubSet(rattrs, size)) {
        LeftFK = true;
        // check if reference key is candidate key of Right input
        if ((*Left->FKeyList[i]->RefKey) == (*Right->CandidateKey)) {
          IsFKJoin = true;
          IsFKSchema = true;
          break;
        } else {
          IsFKJoin = false;
          IsFKSchema = true;
          // get Cucard for reference key;
          //  not sure if it is the correct multiple
          for (int j = 0; j < Left->FKeyList[i]->RefKey->GetSize(); j++) {
            Attribute *Attr = Cat->GetAttr((*Left->FKeyList[i]->RefKey)[j]);
            RefUcard *= Attr->CuCard;
          }
        }
      }
    }
  }

  if (!LeftFK)  // continue to check the right one
  {
    for (i = 0; i < Right->FKeyList.size(); i++) {
      // if rattr contains FK
      if (Right->FKeyList[i]->ForeignKey->IsSubSet(rattrs, size)) {
        // eqjoin on the refkey
        if (Right->FKeyList[i]->RefKey->IsSubSet(lattrs, size)) {
          RightFK = true;
          // check if reference key is candidate key of Left input
          if ((*Right->FKeyList[i]->RefKey) == (*Left->CandidateKey)) {
            IsFKJoin = true;
            IsFKSchema = true;
            break;
          } else {
            IsFKJoin = false;
            IsFKSchema = true;
            // get Cucard for reference key;
            //  not sure if it is the correct multiple
            for (int j = 0; j < Right->FKeyList[i]->RefKey->GetSize(); j++) {
              Attribute *Attr = Cat->GetAttr((*Right->FKeyList[i]->RefKey)[j]);
              RefUcard *= Attr->CuCard;
            }
          }
        }
      }
    }
  }

  double Card;
  double UCard;
  switch (ConditionNum) {
    case 0:  // no join condition, cross-product
      if (Left->Card == -1 || Right->Card == -1)
        Card = -1;
      else
        Card = Left->Card * Right->Card;
      if (Left->UCard == -1 || Right->UCard == -1)
        UCard = -1;
      else
        UCard = Left->UCard * Right->UCard;
      break;

    default:
      if (Left->Card == -1 || Right->Card == -1)
        Card = -1;
      else if (LeftFK) {
        if (IsFKJoin)
          Card = Left->Card;
        else if (IsFKSchema)
          Card = Left->Card * (Right->Card / RefUcard);
        else
          assert(false);
      } else if (RightFK) {
        if (IsFKJoin)
          Card = Right->Card;
        else if (IsFKSchema)
          Card = Right->Card * (Left->Card / RefUcard);
        else
          assert(false);
      } else {
        Card = (Left->Card > Right->Card) ? Left->Card / pow(Right->Card, ConditionNum - 1)
                                          : Right->Card / pow(Left->Card, ConditionNum - 1);
      }

      if (Left->UCard == -1 || Right->UCard == -1)
        UCard = -1;
      else if (LeftFK) {
        if (IsFKJoin)
          UCard = Left->UCard;
        else if (IsFKSchema)
          UCard = Left->UCard * (Right->Card / RefUcard);
        else
          assert(false);
      } else if (RightFK) {
        if (IsFKJoin)
          UCard = Right->UCard;
        else if (IsFKSchema)
          UCard = Right->UCard * (Left->Card / RefUcard);
        else
          assert(false);
      } else
        UCard = (Left->UCard > Right->UCard) ? Left->UCard / pow(Right->UCard, ConditionNum - 1)
                                             : Right->UCard / pow(Left->UCard, ConditionNum - 1);
      break;
  }

  // the candidate key is the merge of two candidate keys from the inputs
  KEYS_SET *cand_key;
  if (IsFKJoin) {
    if (LeftFK) {
      int *temp_key = Left->CandidateKey->CopyOut();
      cand_key = new KEYS_SET(temp_key, Left->CandidateKey->GetSize());
      delete[] temp_key;
    } else if (RightFK) {
      int *temp_key = Right->CandidateKey->CopyOut();
      cand_key = new KEYS_SET(temp_key, Right->CandidateKey->GetSize());
      delete[] temp_key;
    }
  } else {
    int *temp_key = Left->CandidateKey->CopyOut();
    cand_key = new KEYS_SET(temp_key, Left->CandidateKey->GetSize());
    delete[] temp_key;
    cand_key->Merge(*Right->CandidateKey);
  }

  LOG_COLL_PROP *result = new LOG_COLL_PROP((float)Card, (float)UCard, schema, cand_key);

  // foreign key is the merge of left foreign keys and right foreign keys
  for (i = 0; i < Left->FKeyList.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*Left->FKeyList[i]);
    result->FKeyList.push_back(fk);
  }
  for (i = 0; i < Right->FKeyList.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*Right->FKeyList[i]);
    result->FKeyList.push_back(fk);
  }

  return result;
}

ub4 EQJOIN::hash() {
  ub4 hashval = GetInitval();

  // to check the equality of the conditions
  for (int i = size; --i >= 0;) {
    hashval = lookup2(lattrs[i], hashval);
    hashval = lookup2(rattrs[i], hashval);
  }
  return (hashval % (HtblSize - 1));
}
/*********** DUMMY functions ****************/
DUMMY::DUMMY() {
  name = GetName();  // for debug
};

DUMMY::DUMMY(DUMMY &Op) {
  name = Op.name;  // for debug
};

string DUMMY::Dump() { return GetName(); }

//##ModelId=3B0C08740043
LOG_PROP *DUMMY::FindLogProp(LOG_PROP **input) {
  LOG_COLL_PROP *Left = (LOG_COLL_PROP *)(input[0]);
  LOG_COLL_PROP *Right = (LOG_COLL_PROP *)(input[1]);

  assert(Left->Card >= 0);
  assert(Right->Card >= 0);
  assert(Left->UCard >= 0);
  assert(Right->UCard >= 0);

  // union schema
  Schema *Schema = Left->schema->UnionSchema(Right->schema);

  double Card;
  double UCard;
  // no join condition, cross-product
  if (Left->Card == -1 || Right->Card == -1)
    Card = -1;
  else
    Card = Left->Card * Right->Card;
  if (Left->UCard == -1 || Right->UCard == -1)
    UCard = -1;
  else
    UCard = Left->UCard * Right->UCard;

  // the candidate key is the merge of two candidate keys from the inputs
  KEYS_SET *cand_key;
  {
    int *temp_key = Left->CandidateKey->CopyOut();
    cand_key = new KEYS_SET(temp_key, Left->CandidateKey->GetSize());
    delete[] temp_key;
    cand_key->Merge(*Right->CandidateKey);
  }

  LOG_COLL_PROP *result = new LOG_COLL_PROP((float)Card, (float)UCard, Schema, cand_key);
  return result;
}

//##ModelId=3B0C0874006B
ub4 DUMMY::hash() {
  ub4 hashval = GetInitval();

  return (hashval % (HtblSize - 1));
}

/*********** PROJECT functions ****************/

PROJECT::PROJECT(int *attrs, int size) : attrs(attrs), size(size) {
  name = GetName();  // for debug
};

PROJECT::PROJECT(PROJECT &Op) : attrs(CopyArray(Op.attrs, Op.size)), size(Op.size) {
  name = Op.name;  // for debug
};

ub4 PROJECT::hash() {
  ub4 hashval = GetInitval();

  // to check the equality of the conditions
  for (int i = size; --i >= 0;) hashval = lookup2(attrs[i], hashval);

  return (hashval % (HtblSize - 1));
}

//##ModelId=3B0C08740223
LOG_PROP *PROJECT::FindLogProp(LOG_PROP **input) {
  float new_ucard = 1, attr_cucard;

  LOG_COLL_PROP *rel_input = (LOG_COLL_PROP *)(input[0]);

  Schema *schema = rel_input->schema->projection(attrs, size);

  for (int att_index = 0; att_index < schema->GetSize(); att_index++) {
    attr_cucard = (*schema)[att_index]->CuCard;
    if (attr_cucard != -1)
      new_ucard *= attr_cucard;
    else {
      new_ucard = rel_input->Card;
      break;
    }
  }
  new_ucard = MIN(new_ucard, rel_input->Card);

  KEYS_SET *cand_key;
  if (rel_input->CandidateKey->IsSubSet(attrs, size)) {
    int *temp_key = rel_input->CandidateKey->CopyOut();
    cand_key = new KEYS_SET(temp_key, rel_input->CandidateKey->GetSize());
    delete[] temp_key;
  } else
    cand_key = new KEYS_SET();

  LOG_COLL_PROP *result = new LOG_COLL_PROP(rel_input->Card, new_ucard, schema, cand_key);

  // if foreign keys are subset of project attrs, pass this foreign key
  for (int i = 0; i < rel_input->FKeyList.size(); i++) {
    if (rel_input->FKeyList[i]->ForeignKey->IsSubSet(attrs, size))
      result->FKeyList.push_back(new FOREIGN_KEY(*rel_input->FKeyList[i]));
  }

  return result;

}  // PROJECT::FindLogProp

string PROJECT::Dump() {
  string os;
  int i;

  os = GetName() + "(";

  for (i = 0; (size > 0) && (i < size - 1); i++) os += GetAttName(attrs[i]) + ",";

  os += GetAttName(attrs[i]);

  return os;
}

SELECT::SELECT() {
  name = GetName();  // for debug
};

SELECT::SELECT(SELECT &Op) {
  name = Op.name;  // for debug
};

//##ModelId=3B0C08740116
LOG_PROP *SELECT::FindLogProp(LOG_PROP **input) {
  LOG_COLL_PROP *rel_input = (LOG_COLL_PROP *)input[0];
  LOG_ITEM_PROP *pred_input = (LOG_ITEM_PROP *)input[1];

  double old_cucard, new_cucard, new_card;
  double sel = pred_input->Selectivity;

  Schema *new_schema = new Schema(*(rel_input->schema));

  new_card = ceil(rel_input->Card * sel);
  for (int i = 0; i < new_schema->GetSize(); i++) {
    old_cucard = (*new_schema)[i]->CuCard;
    if (old_cucard != -1) {
      new_cucard = ceil(1 / (1 / old_cucard - 1 / (rel_input->Card) + 1 / new_card));

      // "select multiplicity error"
      assert(new_cucard <= old_cucard + 1);

      (*new_schema)[i]->CuCard = (float)new_cucard;
    }
  }

  KEYS_SET *cand_key;
  int *temp_key = rel_input->CandidateKey->CopyOut();
  cand_key = new KEYS_SET(temp_key, rel_input->CandidateKey->GetSize());
  delete[] temp_key;

  LOG_COLL_PROP *result = new LOG_COLL_PROP((float)new_card, (float)ceil(rel_input->UCard * sel), new_schema, cand_key);

  // pass the foreign key info
  for (int i = 0; i < rel_input->FKeyList.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*rel_input->FKeyList[i]);
    result->FKeyList.push_back(fk);
  }

  return result;

}  // SELECT::FindLogProp

//##ModelId=3B0C0874013D
ub4 SELECT::hash() {
  ub4 hashval = GetInitval();

  return (hashval % (HtblSize - 1));
}

string SELECT::Dump() { return GetName(); }

RM_DUPLICATES::RM_DUPLICATES() {
  name = GetName();  // for debug
};

RM_DUPLICATES::RM_DUPLICATES(RM_DUPLICATES &Op) {
  name = Op.name;  // for debug
};

LOG_PROP *RM_DUPLICATES::FindLogProp(LOG_PROP **input) {
  LOG_COLL_PROP *rel_input = (LOG_COLL_PROP *)input[0];

  float new_ucard, new_card;

  Schema *new_schema = new Schema(*(rel_input->schema));

  new_card = rel_input->UCard;
  new_ucard = rel_input->UCard;

  KEYS_SET *cand_key;
  int *temp_key = rel_input->CandidateKey->CopyOut();
  cand_key = new KEYS_SET(temp_key, rel_input->CandidateKey->GetSize());
  delete temp_key;

  LOG_COLL_PROP *result = new LOG_COLL_PROP(new_card, new_ucard, new_schema, cand_key);

  // pass the foreign key info
  for (int i = 0; i < rel_input->FKeyList.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*rel_input->FKeyList[i]);
    result->FKeyList.push_back(fk);
  }

  return result;

}  // RM_DUPLICATES::FindLogProp

ub4 RM_DUPLICATES::hash() {
  ub4 hashval = GetInitval();

  return (hashval % (HtblSize - 1));
}

string RM_DUPLICATES::Dump() { return GetName(); }

AGG_LIST::AGG_LIST(int *gby_atts, int gby_size, AGG_OP_ARRAY *agg_ops)
    : GbyAtts(gby_atts), GbySize(gby_size), AggOps(agg_ops) {
  // produce a flattened list
  if (AggOps) {
    int NumOps = AggOps->size();
    FAttsSize = 0;
    int i, j, index;
    int *TempAtts;
    for (i = 0; i < NumOps; i++) FAttsSize += (*AggOps)[i]->GetAttsSize();
    FlattenedAtts = new int[FAttsSize];
    index = 0;
    for (i = 0; i < NumOps; i++) {
      TempAtts = CopyArray((*AggOps)[i]->GetAtts(), (*AggOps)[i]->GetAttsSize());
      for (j = 0; j < (*AggOps)[i]->GetAttsSize(); j++, index++) FlattenedAtts[index] = TempAtts[j];
      delete[] TempAtts;
    }
  } else {
    FlattenedAtts = 0;
    FAttsSize = 0;
  }

  name = GetName();  // for debug
}

//##ModelId=3B0C087500EE
ub4 AGG_LIST::hash() {
  ub4 hashval = GetInitval();
  int i;

  // to check the equality of the gby attributes
  for (i = GbySize; --i >= 0;) {
    hashval = lookup2(GbyAtts[i], hashval);
  }

  // to check the equality of the FlattenedAtts
  for (i = FAttsSize; --i >= 0;) {
    hashval = lookup2(FlattenedAtts[i], hashval);
  }

  return (hashval % (HtblSize - 1));
}

//##ModelId=3B0C087500BD
LOG_PROP *AGG_LIST::FindLogProp(LOG_PROP **input) {
  LOG_COLL_PROP *rel_input = (LOG_COLL_PROP *)input[0];
  Schema *temp_schema = rel_input->schema->projection(GbyAtts, GbySize);

  float new_cucard, gby_cucard;
  float new_card = 1;
  const float card_reduction_factor = (float)0.7;
  int i;

  bool CuCardKnown = true;
  for (i = 0; i < GbySize; i++) {
    gby_cucard = (*temp_schema)[i]->CuCard;
    // check for overflow
    if (gby_cucard != -1)
      new_card *= gby_cucard;
    else {
      new_card = 1;
      CuCardKnown = false;
      break;
    }
  }
  if (!CuCardKnown) {
    new_card = rel_input->Card * card_reduction_factor;
  }

  new_card = MIN(new_card, rel_input->Card * card_reduction_factor);
  new_cucard = new_card;

  // add ATTR_EXP for every AGG_OP

  int NumOps = AggOps->size();
  Schema *agg_schema = new Schema(NumOps);
  for (i = 0; i < NumOps; i++) {
    AGG_OP *aggop = (*AggOps)[i];
    Attribute *new_attr = new Attribute(aggop->GetRangeVar(), aggop->GetAtts(), aggop->GetAttsSize());
    // CuCard is the same as group by
    new_attr->CuCard = new_cucard;
    agg_schema->AddAttr(i, new_attr);
  }

  agg_schema->TableNum = 1;
  agg_schema->TableId = new int[1];
  agg_schema->TableId[0] = 0;

  Schema *result_schema = temp_schema->UnionSchema(agg_schema);
  delete temp_schema;
  delete agg_schema;

  // new candidate key is:
  // if input candidate_key in GbyAtts, input candidate_key
  // else GbyAtts
  KEYS_SET *cand_key;

  if (rel_input->CandidateKey->IsSubSet(GbyAtts, GbySize)) {
    int *temp_key = rel_input->CandidateKey->CopyOut();
    cand_key = new KEYS_SET(temp_key, rel_input->CandidateKey->GetSize());
    delete temp_key;
  } else
    cand_key = new KEYS_SET(GbyAtts, GbySize);

  LOG_COLL_PROP *result = new LOG_COLL_PROP(new_card, new_cucard, result_schema, cand_key);

  // if foreign keys are subset of gby attrs, pass this foreign key
  for (i = 0; i < rel_input->FKeyList.size(); i++) {
    if (rel_input->FKeyList[i]->ForeignKey->IsSubSet(GbyAtts, GbySize))
      result->FKeyList.push_back(new FOREIGN_KEY(*rel_input->FKeyList[i]));
  }
  return result;
}  // AGG_LIST::FindLogProp

//##ModelId=3B0C087500EF
string AGG_LIST::Dump() {
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
  for (i = 0; i < NumOps - 1; i++) os += (*AggOps)[i]->Dump();
  if (NumOps > 0) {
    os += (*AggOps)[i]->Dump();
    os += " )";
  } else
    os += "Empty set )";

  return os;
}  // AGG_LIST::Dump

bool AGG_LIST::operator==(Operator *other) {
  bool result;
  result = other->GetNameId() == GetNameId() && EqualArray(((AGG_LIST *)other)->GbyAtts, GbyAtts, GbySize);

  // traverse the agg_ops
  if (result) {
    int NumOps = AggOps->size();
    for (int i = 0; i < NumOps && result; i++) {
      AGG_OP *oth_op = (*((AGG_LIST *)other)->AggOps)[i];
      AGG_OP *thi_op = (*AggOps)[i];

      result = (oth_op == thi_op);
    }
  }

  return result;
}  // AGG_LIST::==

//##ModelId=3B0C08750224
LOG_PROP *FUNC_OP::FindLogProp(LOG_PROP **input) {
  LOG_COLL_PROP *rel_input = (LOG_COLL_PROP *)input[0];
  Schema *temp_schema = new Schema(*(rel_input->schema));

  Schema *new_schema = new Schema(1);
  Attribute *new_attr = new Attribute(RangeVar, Atts, AttsSize);
  // the CuCard is the UCard of the input relation
  new_attr->CuCard = rel_input->UCard;
  new_schema->AddAttr(0, new_attr);

  new_schema->TableId = 0;
  new_schema->TableNum = 0;
  Schema *result_schema = temp_schema->UnionSchema(new_schema);
  delete temp_schema;
  delete new_schema;

  KEYS_SET *cand_key;
  int *temp_key = rel_input->CandidateKey->CopyOut();
  cand_key = new KEYS_SET(temp_key, rel_input->CandidateKey->GetSize());
  delete temp_key;

  LOG_COLL_PROP *result = new LOG_COLL_PROP(rel_input->Card, rel_input->UCard, result_schema, cand_key);

  // pass the foreign key info
  for (int i = 0; i < rel_input->FKeyList.size(); i++) {
    FOREIGN_KEY *fk = new FOREIGN_KEY(*rel_input->FKeyList[i]);
    result->FKeyList.push_back(fk);
  }

  return result;
}  // FUNC_OP::FindLogProp

//##ModelId=3B0C08750256
ub4 FUNC_OP::hash() {
  ub4 hashval = GetInitval();

  // to check the equality of the attributes
  for (int i = AttsSize; --i >= 0;) {
    hashval = lookup2(Atts[i], hashval);
  }

  return (hashval % (HtblSize - 1));
}

string FUNC_OP::Dump() {
  string os;
  string temp;
  int i;

  os = GetName() + "(";

  for (i = 0; (AttsSize > 0) && (i < AttsSize - 1); i++) os += GetAttName(Atts[i]) + ",";

  os += GetAttName(Atts[i]) + ")";

  os += " AS " + RangeVar;
  return os;
}
