/*
global.h - global objects
$Revision: 10 $
Columbia Optimizer Framework

  A Joint Research Project of Portland State University
  and the Oregon Graduate Institute
  Directed by Leonard Shapiro and David Maier
  Supported by NSF Grants IRI-9610013 and IRI-9619977
*/

#include "../header/logop.h"
#include "../header/physop.h"
#include "../header/tasks.h"

// Each array maps an integer into the elements of the array at that integer
// location, i.e. maps i to array[i].
// These should probably be in CAT
STRING_ARRAY CollTable;    // CollId to char* name of collection
STRING_ARRAY AttTable;     // AttId to char* name of attribute (including coll name)
STRING_ARRAY IndTable;     // IndId to CollId
INT_ARRAY AttCollTable;    // AttId to CollId
STRING_ARRAY BitIndTable;  //	BitIndex name table

CString SQueryFile = "query";   // query file name
CString BQueryFile = "bquery";  // query file for batch queries
CString CatFile = "catalog";    // catalog file name
CString CMFile = "cm";          // cost model file name
CString RSFile = "ruleset";     // rule set file name
CString AppDir;                 // directory of the application

bool FileTrace = true;       // trace to file flag
bool PiggyBack = false;      // Retain the MEMO structure for use in the subsequent optimization
bool COVETrace = false;      // trace to file flag
bool WindowTrace = false;    // trace to window flag
bool TraceFinalSSP = false;  // global trace flag
bool TraceOPEN = false;      // global trace flag
bool TraceSSP = false;       // global trace flag

bool Pruning = true;           // pruning flag
bool CuCardPruning = true;     // cucard pruning flag
bool GlobepsPruning = false;   // global epsilon pruning flag
int RadioVal = 1;              // the radio value for queryfile
bool SingleLineBatch = false;  // Single line per query in batch mode
bool Halt = false;             // halt flat
int HaltGrpSize = 100;         // halt when number of plans equals to 100% of group
int HaltWinSize = 3;           // window size for checking the improvement
int HaltImpr = 20;             // halt when the improvement is less than 20%

// GLOBAL_EPS can also be set by the options window.
// GLOBAL_EPS is typically determined as a small percentage of
//  a cost found in a first pass optimization.
// Any subplan costing less than this is taken to be optimal.
double GLOBAL_EPS = 0.5;  // global epsilon value
// if GlobalepsPruning is not set, this value is 0
// otherwise, this value will be reset in main
Cost GlobalEpsBound(0);
bool ForGlobalEpsPruning = false;

int TraceDepth = 0;   // global Trace depth
bool TraceOn = true;  // global Trace flag

class OPT_STAT *OptStat;  // Opt statistics object

CLASS_STAT ClassStat[] =  // class statistics object
    {
        CLASS_STAT("AGG_LIST", sizeof(AGG_LIST)),
        CLASS_STAT("AGG_OP", sizeof(AGG_OP)),
        CLASS_STAT("APPLY_RULE", sizeof(APPLY_RULE)),
        CLASS_STAT("ATTR", sizeof(class ATTR)),
        CLASS_STAT("ATTR_EXP", sizeof(class ATTR_EXP)),
        CLASS_STAT("ATTR_OP", sizeof(class ATTR_OP)),
        CLASS_STAT("BINDERY", sizeof(class BINDERY)),
        CLASS_STAT("BIT_JOIN", sizeof(class BIT_JOIN)),
        CLASS_STAT("COMP_OP", sizeof(class COMP_OP)),
        CLASS_STAT("CONST_INT_OP", sizeof(class CONST_INT_OP)),
        CLASS_STAT("CONST_SET_OP", sizeof(class CONST_SET_OP)),
        CLASS_STAT("CONST_STR_OP", sizeof(class CONST_STR_OP)),
        CLASS_STAT("CONT", sizeof(class CONT)),
        CLASS_STAT("Cost", sizeof(class Cost)),
        CLASS_STAT("E_GROUP", sizeof(class E_GROUP)),
        CLASS_STAT("EQJOIN", sizeof(class EQJOIN)),
        CLASS_STAT("EXPR", sizeof(class EXPR)),
        CLASS_STAT("FILE_SCAN", sizeof(class FILE_SCAN)),
        CLASS_STAT("FILTER", sizeof(class FILTER)),
        CLASS_STAT("FUNC_OP", sizeof(class FUNC_OP)),
        CLASS_STAT("GET", sizeof(class GET)),
        CLASS_STAT("GROUP", sizeof(class GROUP)),
        CLASS_STAT("HASH_DUPLICATES", sizeof(class HASH_DUPLICATES)),
        CLASS_STAT("HGROUP_LIST", sizeof(class HGROUP_LIST)),
        CLASS_STAT("INDEXED_FILTER", sizeof(class INDEXED_FILTER)),
        CLASS_STAT("KEYS_SET", sizeof(class KEYS_SET)),
        CLASS_STAT("LEAF_OP", sizeof(class LEAF_OP)),
        CLASS_STAT("LOG_COLL_PROP", sizeof(class LOG_COLL_PROP)),
        CLASS_STAT("LOG_ITEM_PROP", sizeof(class LOG_ITEM_PROP)),
        CLASS_STAT("LOOPS_INDEX_JOIN", sizeof(class LOOPS_INDEX_JOIN)),
        CLASS_STAT("LOOPS_JOIN", sizeof(class LOOPS_JOIN)),
        CLASS_STAT("M_EXPR", sizeof(class M_EXPR)),
        CLASS_STAT("M_WINNER", sizeof(class M_WINNER)),
        CLASS_STAT("MERGE_JOIN", sizeof(class MERGE_JOIN)),
        CLASS_STAT("HASH_JOIN", sizeof(class HASH_JOIN)),
        CLASS_STAT("O_EXPR", sizeof(class O_EXPR)),
        CLASS_STAT("O_GROUP", sizeof(class O_GROUP)),
        CLASS_STAT("O_INPUTS", sizeof(class O_INPUTS)),
        CLASS_STAT("P_FUNC_OP", sizeof(class P_FUNC_OP)),
        CLASS_STAT("P_PROJECT", sizeof(class P_PROJECT)),
        CLASS_STAT("PHYS_PROP", sizeof(class PHYS_PROP)),
        CLASS_STAT("PROJECT", sizeof(class PROJECT)),
        CLASS_STAT("QSORT", sizeof(class QSORT)),
        CLASS_STAT("RM_DUPLICATES", sizeof(class RM_DUPLICATES)),
        CLASS_STAT("SCHEMA", sizeof(class SCHEMA)),
        CLASS_STAT("SELECT", sizeof(class SELECT)),
        CLASS_STAT("WINNER", sizeof(class WINNER)),
};  // class CLASS_STAT

int CLASS_NUM = slotsof(ClassStat);  // sizeof of ClassStat

RULE_SET *RuleSet;      // Rule set
CAT *Cat;               // read catalog in
QUERY *Query;           // read query in
CostModel *Cm;          // read cost model in
SSP *Ssp;               // Create Search space
KEYS_SET IntOrdersSet;  // set of interesting orders

ofstream OutputFile;  // result file
ofstream OutputCOVE;  // script file
PTASKS PTasks;        // pending task

// **************  include physcial mexpr in group or not *****************
bool NO_PHYS_IN_GROUP = false;
// ************************************************************************
