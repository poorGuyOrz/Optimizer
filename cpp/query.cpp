#include "../header/query.h"

#include "../header/stdafx.h"

#define LINEWIDTH 255    // buffer length of one text line
#define MAXLENGTH 10240  // max length of the lisp expression

// Keyword definition
#define KEYWORD_GET "GET"
#define KEYWORD_EQJOIN "EQJOIN"
#define KEYWORD_DUMMY "DUMMY"
#define KEYWORD_PROJECT "PROJECT"
#define KEYWORD_SELECT "SELECT"
#define KEYWORD_OP_NOT "OP_NOT"
#define KEYWORD_OP_AND "OP_AND"
#define KEYWORD_OP_OR "OP_OR"
#define KEYWORD_OP_GE "OP_GE"
#define KEYWORD_OP_LE "OP_LE"
#define KEYWORD_OP_GT "OP_GT"
#define KEYWORD_OP_LT "OP_LT"
#define KEYWORD_OP_EQ "OP_EQ"
#define KEYWORD_OP_NE "OP_NE"
#define KEYWORD_OP_LIKE "OP_LIKE"
#define KEYWORD_OP_IN "OP_IN"
#define KEYWORD_ATTR "Attribute"
#define KEYWORD_STR "STR"
#define KEYWORD_INT "INT"
#define KEYWORD_RM_DUPLICATES "RM_DUPLICATES"
#define KEYWORD_AGG_LIST "AGG_LIST"
#define KEYWORD_AS "AS"
#define KEYWORD_GBY "GROUP_BY"
#define KEYWORD_FUNC_OP "FUNC_OP"
#define KEYWORD_ORDER_BY "ORDER_BY"
#define KEYWORD_ASC "ASC"
#define KEYWORD_DES "DES"
#define KEYWORD_SET "SET"

#define LEFT_BRACKET '('
#define RIGHT_BRACKET ')'
#define DOT '.'
#define COMMA ','
#define BLANKSPACE ' '
#define TABSPACE '\t'
#define QUOTE '"'

#define PARSE_OP1(KEYWORD_OP, Operator)     \
  if (p = strstr(OneElement, KEYWORD_OP)) { \
    Expr = ParseExpr(ExprStr);              \
    free(OneElement);                       \
    Op = new COMP_OP(Operator);             \
    return new Expression(Op, Expr);        \
  }

#define PARSE_OP2(KEYWORD_OP, Operator)             \
  if (p = strstr(OneElement, KEYWORD_OP)) {         \
    LeftExpr = ParseExpr(ExprStr);                  \
    RightExpr = ParseExpr(ExprStr);                 \
    free(OneElement);                               \
    Op = new COMP_OP(Operator);                     \
    return new Expression(Op, LeftExpr, RightExpr); \
  }

Query::Query(string QueryFile) {
  FILE *fp;                  // file handle
  char TextLine[LINEWIDTH];  // text line buffer
  char Buf[MAXLENGTH];       // expression buffer
  char *p;
  int i = 0;
  int LeftNumber = 0;

  if ((fp = fopen(QueryFile.c_str(), "r")) == NULL) OUTPUT_ERROR("can not open file 'query'!");

  // skip the comment lines or blank lines
  TextLine[0] = 0;
  while (IsCommentOrBlankLine(TextLine)) {
    fgets(TextLine, LINEWIDTH, fp);
    if (feof(fp)) OUTPUT_ERROR("no query expression!");
  }

  // store the original query line
  ExprBuf = TextLine;

  // read the whole lisp expression and copy it into ExprBuf,
  // begin with left_bracket and end with right_bracket.
  // it may cover several text lines, so need to get rid of the '\n's
  p = SkipSpace(TextLine);
  if (*p != LEFT_BRACKET)
    OUTPUT_ERROR("Expression must start with '('. In batch mode, expression must initiate with 'Query:'   ");

  p++;  // skip the first left_bracket

  for (;;) {
    while (*p != LEFT_BRACKET && *p != RIGHT_BRACKET && *p != '\n') Buf[i++] = *p++;

    if (*p == LEFT_BRACKET) LeftNumber++;

    if (*p == RIGHT_BRACKET) {
      if (LeftNumber == 0)  // finished: got the last right_bracket
        break;
      else
        LeftNumber--;
    }

    if (*p == '\n')  // get next line
    {
      if (fgets(TextLine, LINEWIDTH, fp) == NULL) OUTPUT_ERROR("last right_bracket not found or mismatch!");

      // store the original query line
      ExprBuf += TextLine;

      p = SkipSpace(TextLine);
      continue;  // not copy '\n'
    }

    Buf[i++] = *p++;  // still copy the bracket
    if (i + 1 >= MAXLENGTH) OUTPUT_ERROR("Query Length Exceeds MAXLENGTH limit");
  }

  Buf[i++] = *p++;  // copy the last ')'
  Buf[i] = 0;

  // now we got a lisp expression, recursively parse it
  char *ExprStr = Buf;
  QueryExpr = ParseExpr(ExprStr);

  fclose(fp);
};

// get the expr from parser
Expression *Query::GetEXPR() { return QueryExpr; }

// free up some occupied memory
Query::~Query() {
  // need to free QueryExpr
  delete QueryExpr;
}

// get an expression
Expression *Query::ParseExpr(char *&ExprStr) {
  Expression *LeftExpr, *RightExpr;
  Expression *Expr;
  Operator *Op;
  char *p;
  char *OneElement;

  OneElement = GetOneElement(ExprStr);

  p = SkipSpace(OneElement);
  if (*p == LEFT_BRACKET)  // sub_expression
  {
    p++;  // skip left_bracket
    Expr = ParseExpr(p);
    free(OneElement);
    return Expr;
  }

  if (p = strstr(OneElement, KEYWORD_PROJECT))  // PROJECT
  {
    Expr = ParseExpr(ExprStr);

    p += strlen(KEYWORD_PROJECT);

    KEYS_SET KeysSet;
    ParsePJKeys(p, KeysSet);
    free(OneElement);

    Op = new PROJECT(KeysSet.CopyOut(), KeysSet.GetSize());

    return new Expression(Op, Expr);
  }

  if (p = strstr(OneElement, KEYWORD_SELECT))  // SELECT
  {
    LeftExpr = ParseExpr(ExprStr);   // parse table
    RightExpr = ParseExpr(ExprStr);  // parse predicate

    free(OneElement);

    Op = new SELECT;

    return new Expression(Op, LeftExpr, RightExpr);
  }

  if (p = strstr(OneElement, KEYWORD_EQJOIN))  // EQJOIN
  {
    LeftExpr = ParseExpr(ExprStr);
    RightExpr = ParseExpr(ExprStr);

    p += strlen(KEYWORD_EQJOIN);

    KEYS_SET LeftKeysSet, RightKeysSet;
    ParseKeys(p, LeftKeysSet, RightKeysSet);
    free(OneElement);

    int Size = LeftKeysSet.GetSize();
    assert(Size == RightKeysSet.GetSize());
    Op = new EQJOIN(LeftKeysSet.CopyOut(), RightKeysSet.CopyOut(), Size);

    // if the eqjoin is on multiattribute, choose those attributes that have max CuCard
    if (Size > 1) {
      int win = LeftKeysSet.ChMaxCuCard();

      for (int i = 0; i < Size; i++)
        if (i != win) {
          LeftKeysSet.RemoveKeysSet(i);
          RightKeysSet.RemoveKeysSet(i);
        }
    }

    // merge the left and right key sets into one set
    IntOrdersSet.Merge(LeftKeysSet);
    IntOrdersSet.Merge(RightKeysSet);

    return new Expression(Op, LeftExpr, RightExpr);
  }

  if (p = strstr(OneElement, KEYWORD_DUMMY))  // DUMMY
  {
    LeftExpr = ParseExpr(ExprStr);
    RightExpr = ParseExpr(ExprStr);

    p += strlen(KEYWORD_DUMMY);

    free(OneElement);

    Op = new DUMMY();

    return new Expression(Op, LeftExpr, RightExpr);
  }

  if (p = strstr(OneElement, KEYWORD_GET))  // GET
  {
    p += strlen(KEYWORD_GET);
    p = SkipSpace(p);
    p++;                                // skip '('
    string Str = ParseOneParameter(p);  // Collection name
    string Str2;                        // possible range variable name
    p++;                                // skip " after first parameter
    p = SkipSpace(p);
    if (*p == ')')  // One parameter in GET
      Op = new GET(GetCollId(Str));
    else if (*p == ',')  // Two parameters in GET
    {
      p++;  // skip ,
      Str2 = ParseOneParameter(p);
      Op = new GET(Str, Str2);
    } else
      OUTPUT_ERROR(" GET is missing a COMMA !");

    free(OneElement);
    return new Expression(Op);
  }

  // Deal with item epxrs
  if (p = strstr(OneElement, KEYWORD_ATTR))  // Attribute
  {
    p += strlen(KEYWORD_ATTR);
    p = SkipSpace(p);
    p++;  // skip '('
    Op = new ATTR_OP(GetAttId(ParseOneParameter(p)));
    free(OneElement);

    return new Expression(Op);
  }

  if (p = strstr(OneElement, KEYWORD_INT))  // INT
  {
    p += strlen(KEYWORD_INT);
    p = SkipSpace(p);
    p++;  // skip '('
    Op = new CONST_INT_OP(atoi(ParseOneParameter(p).c_str()));
    free(OneElement);

    return new Expression(Op);
  }

  if (p = strstr(OneElement, KEYWORD_STR))  // STR
  {
    p += strlen(KEYWORD_STR);
    p = SkipSpace(p);
    p++;  // skip '('
    Op = new CONST_STR_OP(ParseOneParameter(p));
    free(OneElement);

    return new Expression(Op);
  }

  if (p = strstr(OneElement, KEYWORD_SET))  // SET
  {
    p += strlen(KEYWORD_SET);
    p = SkipSpace(p);
    p++;  // skip '('
    Op = new CONST_SET_OP(ParseOneParameter(p));
    free(OneElement);

    return new Expression(Op);
  }

  if (p = strstr(OneElement, KEYWORD_RM_DUPLICATES))  // RM_DUPLICATES
  {
    Expr = ParseExpr(ExprStr);  // parse table

    free(OneElement);

    Op = new RM_DUPLICATES;

    return new Expression(Op, Expr);
  }

  if (p = strstr(OneElement, KEYWORD_AGG_LIST))  // AGG_LIST
  {
    Expr = ParseExpr(ExprStr);

    p += strlen(KEYWORD_AGG_LIST);

    p = SkipSpace(p);
    AGG_OP_ARRAY *AggOps = new AGG_OP_ARRAY;
    p++;  // skip '('
    ParseAggOps(p, *AggOps);
    p++;  // skip ','
    KEYS_SET GbyKeysSet;
    ParseGby(p, GbyKeysSet);
    p++;  // skip ')'

    free(OneElement);

    int Size = GbyKeysSet.GetSize();

    int NumOps = AggOps->size();
    Op = new AGG_LIST(GbyKeysSet.CopyOut(), Size, AggOps);

    return new Expression(Op, Expr);
  }

  if (p = strstr(OneElement, KEYWORD_FUNC_OP))  // FUNC_OP
  {
    Expr = ParseExpr(ExprStr);

    p += strlen(KEYWORD_FUNC_OP);

    string range_var;
    KEYS_SET AttrKeySet;

    p = SkipSpace(p);
    p++;  // skip '('

    GetOneKeys(p, AttrKeySet);

    int size = AttrKeySet.GetSize();
    p = SkipSpace(p);
    if (strncmp(p, KEYWORD_AS, strlen(KEYWORD_AS)) != 0) OUTPUT_ERROR("(AGG_LIST, AGG_OP) format missing AS !");
    p += strlen(KEYWORD_AS);  // skip 'as'
    p = SkipSpace(p);

    // get RangeVar
    while (*p != RIGHT_BRACKET && *p != COMMA && *p != BLANKSPACE && *p != TABSPACE) range_var += *p++;

    p++;  // skip ')'
    p++;  // skip ','

    free(OneElement);

    Op = new FUNC_OP(range_var, AttrKeySet.CopyOut(), size);

    return new Expression(Op, Expr);
  }

  if (p = strstr(OneElement, KEYWORD_ORDER_BY))  // ORDER_BY
  {
    Expr = ParseExpr(ExprStr);

    p += strlen(KEYWORD_ORDER_BY);

    p = SkipSpace(p);
    p++;  // skip '('
    KEYS_SET *KeysSet = new KEYS_SET();
    GetOneKeys(p, *KeysSet);

    IntOrdersSet.Merge(*KeysSet);
    p = SkipSpace(p);

    PHYS_PROP *sort_prop = new PHYS_PROP(KeysSet, sorted);

    // read the KeyOrder, if no order specified, defalt is ascending
    while (*p != '<' && *p != ')') p++;  // skip the char before '<'
    if (*p == ')') {
      for (int i = 0; i < KeysSet->GetSize(); i++) sort_prop->KeyOrder.push_back(ascending);
    } else {
      char *str;

      if (*p != '<') OUTPUT_ERROR("can't find '(' when parse KEYS_ORDER!");
      p++;

      while (*p != '>') {
        str = p;
        p = SkipSpace(p);
        while (*p != ' ' && *p != '\t' && *p != ',' && *p != '>' && *p != '\n') p++;  // keep the char
        if (*p == '>') {
          *p = 0;  // get a str
          str = SkipSpace(str);
          sort_prop->KeyOrder.push_back(atoKeyOrder(str));  // add the order to KeyOrder
          break;
        }
        if (*p == ',') {
          *p = 0;                                           // get a str
          sort_prop->KeyOrder.push_back(atoKeyOrder(str));  // add the order to KeyOrder
          *p = ',';                                         // restore the char
          p++;
          continue;
        }
        // else
        OUTPUT_ERROR("format error when parse KEYS_ORDER!");
      }
    }

    CONT *InitCont = new CONT(sort_prop, new Cost(-1), false);
    // Make this the first context
    CONT::vc.push_back(InitCont);
    assert(CONT::vc.size() == 1);

    free(OneElement);

    return Expr;
  }

  // macro to parse COMP_OP
  PARSE_OP1(KEYWORD_OP_NOT, OP_NOT);  // OP_NOT

  PARSE_OP2(KEYWORD_OP_AND, OP_AND);  // OP_AND

  PARSE_OP2(KEYWORD_OP_OR, OP_OR);  // OP_OR

  PARSE_OP2(KEYWORD_OP_EQ, OP_EQ);  // OP_EQ

  PARSE_OP2(KEYWORD_OP_NE, OP_NE);  // OP_NE

  PARSE_OP2(KEYWORD_OP_GE, OP_GE);  // OP_GE

  PARSE_OP2(KEYWORD_OP_LE, OP_LE);  // OP_LE

  PARSE_OP2(KEYWORD_OP_GT, OP_GT);  // OP_GE

  PARSE_OP2(KEYWORD_OP_LT, OP_LT);  // OP_LE

  PARSE_OP2(KEYWORD_OP_LIKE, OP_LIKE);  // OP_LIKE

  PARSE_OP2(KEYWORD_OP_IN, OP_IN);  // OP_IN

  OUTPUT_ERROR("Operator not support!");
  return NULL;
}

// get one element of the expression
char *Query::GetOneElement(char *&Expr) {
  string Str;
  char *OneElement;
  int LeftNumber = 0;

  if (*Expr == 0) return NULL;

  Expr = SkipSpace(Expr);

  for (;;) {
    while (*Expr != LEFT_BRACKET && *Expr != RIGHT_BRACKET && *Expr != COMMA) Str += *Expr++;

    if (*Expr == LEFT_BRACKET) {
      LeftNumber++;
      Str += *Expr++;
      continue;
    }

    if (*Expr == COMMA) {
      if (LeftNumber != 0)
        Str += *Expr++;  // not finish, copy ','
      else {
        Expr++;  // finished,  skip ','
        break;
      }

      continue;
    }

    if (*Expr == RIGHT_BRACKET) {
      Str += *Expr++;  // always copy the ')'
      if (LeftNumber != 0)
        LeftNumber--;
      else {  // finished. may need to skip right ','
        Expr = SkipSpace(Expr);
        if (*Expr != 0) Expr++;
        break;
      }
    }
  }

  OneElement = strdup(Str.c_str());

  return OneElement;
}

// get an GET string
string Query::ParseOneParameter(char *&p) {
  string Str;

  p = SkipSpace(p);
  if (*p != QUOTE)
    OUTPUT_ERROR(
        "GET("
        "A"
        ") format missing QUOTE !");

  p++;  // skip '"'
  while (*p != QUOTE) Str += *p++;

  return Str;
}

// get the project keys
void Query::ParsePJKeys(char *&p, KEYS_SET &Keys) {
  p = SkipSpace(p);
  p++;  // skip '('

  while (*p != RIGHT_BRACKET) {
    GetKey(p, Keys);
    if (*p == COMMA) p++;  // skip ','
  }

  p = SkipSpace(p);
  p++;  // skip ')'
}

// get the AGG_OP_ARRAY
void Query::ParseAggOps(char *&p, AGG_OP_ARRAY &AggOps) {
  p = SkipSpace(p);
  p++;  // skip '('

  while (*p != RIGHT_BRACKET) {
    AggOps.push_back(GetOneAggOp(p));
    if (*p == COMMA) p++;  // skip ','
  }

  p = SkipSpace(p);
  p++;  // skip ')'
}

// get the group by keys
void Query::ParseGby(char *&p, KEYS_SET &Keys) {
  p = SkipSpace(p);
  p++;  // skip '('

  if (strncmp(p, KEYWORD_GBY, strlen(KEYWORD_GBY)) != 0) OUTPUT_ERROR("(AGG_LIST, GROUP_BY) format missing GROUP_BY !");
  p += strlen(KEYWORD_GBY);  // skip 'group_by'
  p = SkipSpace(p);

  GetOneKeys(p, Keys);

  IntOrdersSet.Merge(Keys);  // add group_by keys to the interesting
                             // order set

  p = SkipSpace(p);
  p++;  // skip ')'
}

// get two KEYSs
void Query::ParseKeys(char *p, KEYS_SET &Keys1, KEYS_SET &Keys2) {
  p = SkipSpace(p);
  p++;  // skip '('

  GetOneKeys(p, Keys1);  // parse left keys

  p = SkipSpace(p);
  p++;  // skip ','

  GetOneKeys(p, Keys2);  // parse right keys

  p = SkipSpace(p);
  p++;  // skip ')'
}

// get one KEYS_SET
void Query::GetOneKeys(char *&p, KEYS_SET &Keys) {
  bool MoreKey = false;

  p = SkipSpace(p);
  if (*p == '<') {
    p++;
    MoreKey = true;
  }

  GetKey(p, Keys);

  while (MoreKey) {
    p = SkipSpace(p);
    if (*p == '>')  // finished
    {
      p++;  // skip '>'
      break;
    }

    p++;  // else, skip ','
    GetKey(p, Keys);
  }
}

// get one KeySET, and add to KEYS_SET
void Query::GetKey(char *&p, KEYS_SET &Keys) {
  string Col, Attr;

  p = SkipSpace(p);
  // for empty key set join(EQJOIN(<>,<>) return empty key set
  if (*p == '>') return;

  if (*p == 0 || *p == RIGHT_BRACKET) OUTPUT_ERROR("missing Join KEYS_SET!");

  while (*p != DOT && *p != RIGHT_BRACKET && *p != COMMA) Col += *p++;

  if (*p == RIGHT_BRACKET || *p == COMMA) OUTPUT_ERROR("wrong KEYS_SET format, missing DOT. ");

  p++;  // skip the '.'
  while (*p != RIGHT_BRACKET && *p != COMMA && *p != '>' && *p != BLANKSPACE && *p != TABSPACE) Attr += *p++;

  Keys.AddKey(Col, Attr);
}

//	get one AGG_OP
AGG_OP *Query::GetOneAggOp(char *&p) {
  string range_var;
  KEYS_SET AttrKeySet;

  GetOneKeys(p, AttrKeySet);

  int size = AttrKeySet.GetSize();
  p = SkipSpace(p);
  if (strncmp(p, KEYWORD_AS, strlen(KEYWORD_AS)) != 0) OUTPUT_ERROR("(AGG_LIST, AGG_OP) format missing AS !");
  p += strlen(KEYWORD_AS);  // skip 'as'
  p = SkipSpace(p);

  // get RangeVar
  while (*p != RIGHT_BRACKET && *p != COMMA && *p != BLANKSPACE && *p != TABSPACE) range_var += *p++;

  return (new AGG_OP(range_var, AttrKeySet.CopyOut(), size));
}

string Query::Dump() { return QueryExpr->Dump(); }

// display the interesting orders
string Query::Dump_IntOrders() { return (IntOrdersSet.Dump()); }
