/*
DEFS.H - General programming support: #includes, #defines, typedefs, tracing.
$Revision: 21 $
Columbia Optimizer Framework

  A Joint Research Project of Portland State University
  and the Oregon Graduate Institute
  Directed by Leonard Shapiro and David Maier
  Supported by NSF Grants IRI-9610013 and IRI-9619977
*/

#pragma once

#include "../header/stdafx.h"

/*
============================================================
General definitions
============================================================
*/
#define KILO 1024  // For TPC-D sizes
#define LINE_STRING ("===========")

#define MIN(a, b) ((a > b) ? b : a)
#define MAX(a, b) ((a > b) ? a : b)

/* number of slots in a (locally defined) array */
#define slotsof(ARRAY) (sizeof(ARRAY) / sizeof(ARRAY[0]))

// needed for hashing, used for duplicate elimination.
// See ../doc/dupelim and ../doc/dupelim.pcode
#define LOG2HTBL 13               // LOG2 of number of hash buckets to hold mexprs.
#define HtblSize (1 << LOG2HTBL)  // hash table size we used
typedef unsigned long int ub4;    /* unsigned 4-byte quantities */
typedef unsigned char ub1;        /* unsigned 1-byte quantities */

typedef vector<string> STRING_ARRAY;
typedef vector<int> INT_ARRAY;

typedef unsigned int BIT_VECTOR;  // Used to implement unique rule set.  Note this
// restricts the number of transformational (logical) rules.

extern bool ForGlobalEpsPruning;  // If true, we are running the optimizer to get an
// estimated cost to use for global epsilon pruning.

typedef enum ORDER_AD { ascending, descending } ORDER_AD;

typedef vector<ORDER_AD> KeyOrderArray;

// any means any property
typedef enum ORDER {
  any,
  heap,
  sorted,
  hashed  // assume unique hash function for entire system
} ORDER;

typedef enum ORDER_INDEX {
  btree,
  hash  // assume unique hash function for entire system
} ORDER_INDEX;

// unknown is needed for attributes generated along the query tree
// e.g. sum(xxx) as .SUM, whose domain is unknown
typedef enum DOM_TYPE { string_t, int_t, real_t, unknown } DOM_TYPE;

#define PTRACE(object)                                                                                       \
  {                                                                                                          \
    if (TraceOn && !ForGlobalEpsPruning) {                                                                   \
      if (FileTrace)                                                                                         \
        OutputFile << TraceDepth << ":" << setiosflags(ios::right) << setw(12) << __FILE__ << ":" << setw(4) \
                   << __LINE__ << setiosflags(ios::right) << setw(8) << ">>>>>: " << object << endl;         \
    }                                                                                                        \
  }

// Print n tabs, then the character string.  No newlines except as in string.
#define OUTPUTN(n, str)                                         \
  {                                                             \
    if (!ForGlobalEpsPruning) {                                 \
      string OutputString;                                      \
      OutputString = "    ";                                    \
      for (int i = 0; i < n; i++) OutputString += OutputString; \
      OutputString += str;                                      \
      OutputFile << (OutputString);                             \
    }                                                           \
  }

// trace without line and file info.  No newlines except in format input.
#define WTRACE(format, object)                       \
  {                                                  \
    if (TraceOn) {                                   \
      if (FileTrace) OutputFile << (object) << endl; \
    }                                                \
  }

// Output the object to window and OutputFile, even if tracing is off.
// No newlines except in format input.
// First one writes to window and trace file, second to file only.
#define OUTPUT(object)                \
  {                                   \
    { OutputFile << object << endl; } \
  }

// display error message to Window and OutputFile
#define OUTPUT_ERROR(text)                                                                                \
  {                                                                                                       \
    OutputFile << "\r\nERROR:" << text << ",file:" << (__FILE__) << ",line:" << __LINE__ << "\n" << endl; \
    abort();                                                                                              \
  }

/* ==========  Optimizer related ============  */

class Query;
class OptimizerTaskStack;
class SearchSpace;
class CAT;
class RULE;
class OPT_STAT;
class SET_TRACE;
class RuleSet;
class CostModel;
class KEYS_SET;
class MExression;

extern OPT_STAT *OptStat;  // stat. info. of Optimizer
extern int CLASS_NUM;

extern INT_ARRAY TopMatch;
extern INT_ARRAY Bindings;
extern INT_ARRAY Conditions;

extern STRING_ARRAY CollTable;    // collection name table
extern STRING_ARRAY AttTable;     // attribute name table
extern STRING_ARRAY IndTable;     // index name table
extern INT_ARRAY AttCollTable;    // attribute to collid table
extern STRING_ARRAY BitIndTable;  // Bitind name table

extern ofstream OutputFile;  // global output file
extern ofstream OutputCOVE;  // global COVE script file
extern int TraceDepth;       // Not the stack depth, but the number of times SET_TRACE
// objects have been created in current stack functions.
extern bool TraceOn;          // Are we tracing?
extern bool FileTrace;        // Are we sending the trace output to the output file?
extern bool COVETrace;        // Are we doing COVE tracing?
extern bool WindowTrace;      // Are we sending the tracing to the Window?
extern bool TraceFinalSSP;    // Does the trace print the final search space?
extern bool TraceOPEN;        // Should we force tracing of the OPEN stack?
extern bool TraceSSP;         // Should we force tracing of the Search Space?
extern bool Pruning;          // global pruning flag
extern bool CuCardPruning;    // global cucard pruning flag
extern bool GlobepsPruning;   // global epsilon pruning flag
extern int RadioVal;          // Radio value for queryfile
extern bool SingleLineBatch;  // Should output of batch queries be one line per query?
// SingleLineBatch and _TABLE_ cannot both be true
extern bool Halt;         // halt flat
extern int HaltGrpSize;   // halt when number of plans equals to 100% of group
extern int HaltWinSize;   // window size for checking the improvement
extern int HaltImpr;      // halt when the improvement is less than 20%
extern int TaskNo;        // Number of the current task.
extern int Memo_M_Exprs;  // How Many M_EXPRs in the MEMO Structure?

extern double GLOBAL_EPS;  // global epsilon value

extern int printnx;

extern Query *query;
extern OptimizerTaskStack PTasks;
extern SearchSpace *Ssp;
extern CAT *Cat;
extern RuleSet *ruleSet;
extern CostModel *costModel;
extern KEYS_SET IntOrdersSet;  // set of Interesting Orders

extern bool NO_PHYS_IN_GROUP;

/* ======= Pragmas ====== */

/* ======= Symbolic Constants ====== */

/*

UNIQ:   on == Use the unique rule set rules
SORT_AFTERS: sort possible moves in order of estimated cost
INFBOUND: When optimizing a group, ignore the initial upper bound; use infinity instead
_COSTS_ Prints the cost of each mexpr as it is costed, in the output window
_TABLE_: prints one summary line for each optimization, using different epsilons
_GEN_LOG: Used to control the generation of logical expressions when eps pruning is done.
REUSE_SIB: An attempt to improve pattern matching by reusing one side of generated mexprs.
NOCART: Do not allow Cartesian products during optimization.
SAFETY: Used within Bill's memory manager.  Higher level of error checking.
CONDPRUNE: Use Group Pruning technique as a condition for firing some rules
*/
