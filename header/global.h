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

string SQueryFile = "query";   // query file name
string BQueryFile = "bquery";  // query file for batch queries
string CatFile = "catalog";    // catalog file name
string CMFile = "cm";          // cost model file name
string AppDir;                 // directory of the application

bool FileTrace = true;       // trace to file flag
bool PiggyBack = false;      // Retain the MEMO structure for use in the subsequent optimization
bool COVETrace = true;       // trace to file flag
bool WindowTrace = false;    // trace to window flag
bool TraceFinalSSP = false;  // global trace flag
bool TraceOPEN = false;      // global trace flag
bool TraceSSP = false;       // global trace flag

int printnx = 0;

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

RuleSet *ruleSet;       // Rule set
CAT *Cat;               // read catalog in
Query *query;           // read query in
CostModel *costModel;   // read cost model in
SearchSpace *Ssp;               // Create Search space
KEYS_SET IntOrdersSet;  // set of interesting orders

ofstream OutputFile;        // result file
ofstream OutputCOVE;        // script file
OptimizerTaskStack PTasks;  // pending task

// **************  include physcial mexpr in group or not *****************
bool NO_PHYS_IN_GROUP = false;
// ************************************************************************
