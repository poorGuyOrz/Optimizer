// main.cpp -  main file of the columbia optimizer

#include "../header/cat.h"
#include "../header/cm.h"
#include "../header/global.h"  // global variables
#include "../header/physop.h"
#include "../header/stdafx.h"
#include "../header/tasks.h"

#define LINEWIDTH 256  // buffer length of one text line
#define KEYWORD_NUMOFQRY "NumOfQuery:"
#define KEYWORD_QUERY "Query:"
#define KEYWORD_PIGGYBACK "PiggyBack"

// Rule Firing Statistics
INT_ARRAY TopMatch;
INT_ARRAY Bindings;
INT_ARRAY Conditions;

void Optimizer() {
  TaskNo = 0;
  Memo_M_Exprs = 0;
  SET_TRACE Trace(true);
  AppDir = "../";
  if (CatFile == "catalog") CatFile = AppDir + "CATALOGS/uniform.txt";
  if (CMFile == "cm") CMFile = AppDir + "CMS/cmj";

  // Open general trace file and COVE trace file, clear main output window
  OutputFile.open((AppDir + "/colout.txt"));
  OutputCOVE.open((AppDir + "/script.cove"));

  // clean the statistics
  for (int i = 0; i < CLASS_NUM; i++) {
    ClassStat[i].Count = 0;
    ClassStat[i].Max = 0;
    ClassStat[i].Total = 0;
  }

  // Create objects to manage Opt stats, Cost model, Rule set, Heuristic cost.
  OptStat = new OPT_STAT;
  costModel = new CostModel(CMFile);
  PTRACE("cost model content: " << endl << costModel->Dump());
  ruleSet = new RuleSet();
  PTRACE("Rule set content:" << endl << ruleSet->Dump());
  Cost *HeuristicCost;
  HeuristicCost = new Cost(0);

  // Initialize Rule Firing Statistics
  TopMatch.resize(ruleSet->RuleCount);
  Bindings.resize(ruleSet->RuleCount);
  Conditions.resize(ruleSet->RuleCount);  // 625
  for (int RuleNum = 0; RuleNum < ruleSet->RuleCount; RuleNum++) {
    TopMatch[RuleNum] = 0;
    Bindings[RuleNum] = 0;
    Conditions[RuleNum] = 0;
  }

  // SQueryFile, BQueryFile have been set to the Single or Batch Query File chosen in
  //   the option dialog.  The CString QueryFile will be set to the name of a file
  //   containing the query currently being optimized.  In the single query case that
  //   will be SQueryFile, in the Batch Query case it will be "tempquery", which will
  //   contain the current batch query, copied from BQueryFile.  The default case is
  //   handled separately, with files named query and bquery.
  // NumQuery will be set here to the number of queries.
  string QueryFile;
  int NumQuery;
  FILE *fp;                  // for the batch query file BQueryFile
  char TextLine[LINEWIDTH];  // text line buffer
  char *p;
  if (RadioVal == 1)  // Single Query case
  {
    if (SQueryFile == "query")
      QueryFile = AppDir + "testquery.cl";  // default case
    else
      QueryFile = SQueryFile;  // value entered in option dialog; should check it exists.
    NumQuery = 1;
  } else if (RadioVal == 0)  // Batch Query case
  {
    // Open BQueryFile
    if (BQueryFile == "bquery") BQueryFile = AppDir + "testquery.cl";  // default case
    if ((fp = fopen(BQueryFile.c_str(), "r")) == NULL)
      OUTPUT_ERROR("can not open the file you chose in the option dialogue");
    fgets(TextLine, LINEWIDTH, fp);
    if (feof(fp)) OUTPUT_ERROR("Empty Input File");
  }

  // The following loop is executed once for the single query case.
  // For the batch case, it is executed once for each batch query
  // sequence. Executes batch of batches too.
  do {
    if (RadioVal == 0)  // Batch Query case
    {
      // Set NumQuery = Number of Queries in the batch
      // Set the PiggyBack flag to be true if the batch
      // query needs to be PiggyBacked,else the PiggyBack
      // flag remains false.

      PiggyBack = false;
      for (;;) {
        // skip the comment line
        if (IsCommentOrBlankLine(TextLine)) {
          fgets(TextLine, LINEWIDTH, fp);
          continue;
        }

        p = SkipSpace(TextLine);
        if (p == strstr(p, KEYWORD_PIGGYBACK)) {
          p += strlen(KEYWORD_PIGGYBACK);
          p = SkipSpace(p);

          PiggyBack = true;

          fgets(TextLine, LINEWIDTH, fp);

          continue;
        }
        if (p == strstr(p, KEYWORD_NUMOFQRY)) {
          p += strlen(KEYWORD_NUMOFQRY);
          p = SkipSpace(p);
          parseString(p);

          NumQuery = atoi(p);  // NumQuery is set here
          break;
        } else
          OUTPUT_ERROR("the first line in the batch file should be number of queries");
      }
    }

    // Header for single line batch output
    if (SingleLineBatch) OUTPUT("#\tTTask\tTGrp\tCME\tTME\tFR\tCOST\n");

    // For each query numbered q
    bool first = true;  // treat the first "Query: n" specially
    for (int q = 0; q < NumQuery; q++) {
      if (RadioVal == 0)  // Batch case
      {
        // reset the interesting queries & M_WINNERs, clean the statistics
        IntOrdersSet.reset();
#ifdef IRPROP
        // In PiggyBack mode remove the winners only when the first
        // query in a batch is created,for the remaining queries read in keep
        // the winners created for previous queries optimized
        if ((PiggyBack && (0 == q)) || (!PiggyBack)) {
          M_WINNER::mc.RemoveAll();
        }

#endif

        for (int i = 0; i < CLASS_NUM; i++) ClassStat[i].Count = ClassStat[i].Max = ClassStat[i].Total = 0;
        OptStat->DupMExpr = OptStat->FiredRule = OptStat->HashedMExpr = 0;
        OptStat->MaxBucket = OptStat->TotalMExpr = 0;
        for (int RuleNum = 0; RuleNum < ruleSet->RuleCount; RuleNum++) {
          TopMatch[RuleNum] = 0;
          Bindings[RuleNum] = 0;
          Conditions[RuleNum] = 0;
        }
        TaskNo = 0;
        Memo_M_Exprs = 0;

        QueryFile = AppDir + "tempquery";
        FILE *tempfp;
        if ((tempfp = fopen(QueryFile.c_str(), "w")) == NULL)
          OUTPUT_ERROR("can not create or truncate file 'tempquery'");
        // QueryFile.ReleaseBuffer();
        /*
        here the optimizer reads in one query at a time
        until the keyword KEYWORD_QUERY,KEYWORD_NUMOFQRY
        or KEYWORD_PIGGYBACK or end of a file is encountered
        from each query, be it from a single,batch or batch
        of batches query and stores it in a file called tempquery.
        The line containing keyword KEYWORD_QUERY,KEYWORD_NUMOFQRY
        or KEYWORD_PIGGYBACK serves as a demarcator for the optimizer
        to break out of the for loop ,process and optimize the query
        last read before reentering the loop to write another query
        ,if any ,starting reading after the line containing the
        keyword KEYWORD_QUERY,KEYWORD_NUMOFQRY
        or KEYWORD_PIGGYBACK,to the tempquery file to be processed
        and optimized

        */
        for (;;) {
          fgets(TextLine, LINEWIDTH, fp);
          if (feof(fp)) {
            fprintf(tempfp, "%s", TextLine);
            break;
          }
          if (IsCommentOrBlankLine(TextLine)) continue;
          p = SkipSpace(TextLine);
          if (p == strstr(p, KEYWORD_QUERY) || p == strstr(p, KEYWORD_NUMOFQRY) || p == strstr(p, KEYWORD_PIGGYBACK)) {
            if (first) {
              first = false;
              continue;
            } else
              break;
          } else
            fprintf(tempfp, "%s", TextLine);
        }
        fclose(tempfp);
      }

      // Print the number of the query
#ifndef _TABLE_
      if (SingleLineBatch) {
        OUTPUT(q << "\t");  // First entry in output line in window
      } else
        OUTPUT("query: " << q + 1 << "\n");  // In this case it's a full line
#endif

      // if GlobepsPruning, run optimizer without globepsPruning
      // to get the heuristic cost
      if (GlobepsPruning) {
        GlobepsPruning = false;
        ForGlobalEpsPruning = true;
        Cat = new CAT(CatFile);
        query = new Query(QueryFile);
        Ssp = new SSP;
        Ssp->Init();
        delete query;
        Ssp->optimize();
        PHYS_PROP *PhysProp = CONT::vc[0]->GetPhysProp();
        *HeuristicCost = *(Ssp->GetGroup(0)->GetWinner(PhysProp)->GetCost());
        assert(Ssp->GetGroup(0)->GetWinner(PhysProp)->GetDone());
        GlobalEpsBound = (*HeuristicCost) * (GLOBAL_EPS);
        delete Ssp;
        for (int i = 0; i < CONT::vc.size(); i++) delete CONT::vc[i];
        CONT::vc.clear();
        delete Cat;
        GlobepsPruning = true;
        ForGlobalEpsPruning = false;
      }

      // Since each optimization corrupts the catalog, we must create it anew
      Cat = new CAT(CatFile);
      PTRACE("Catalog content:" << endl << Cat->Dump());

#ifdef _TABLE_
      assert(!SingleLineBatch);  // These are incompatible

      //	Print Heading: EPS ...
      OUTPUT("EPS, EPS_BD, CUREXPR, TOTEXPR, OptimizerTask, OPTCOST\n");

      // For each iteration of the global epsilon counter ii {
      for (double ii = 0; ii <= GLOBAL_EPS * 10; ii++) {
        OUTPUT("%3.1f\t", ii / 10);
        GlobalEpsBound = (*HeuristicCost) * ii / 10;
        ClassStat[C_M_EXPR].Count = ClassStat[C_M_EXPR].Total = 0;
#endif

        // Parse and print the query and its interesting orders
        query = new Query(QueryFile);
        PTRACE("Original query:" << endl << query->Dump());
        PTRACE("The interesting orders in the query are:" << endl << query->Dump_IntOrders());

        // Initialize and print the search space, delete the query
        //  In PiggyBack mode create the search space only for
        //  the first query(q == 0)and keep expanding it as more and
        //  more queries are read
        // In non PiggyBack mode create a new search space(Ssp) for
        // every query read because the search space for the previous
        // query is deleted after it is optimized
        if ((PiggyBack && (0 == q)) || (!PiggyBack)) {
          Ssp = new SSP;
        }

        Ssp->Init();
        PTRACE("Initial Search Space:" << endl << Ssp->Dump());
        delete query;

        // Keep track of initial space and time
        PTRACE("---1--- memory statistics before optimization: " << DumpStatistics());
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        auto timet = chrono::system_clock::to_time_t(now);
#ifndef _TABLE_
        if (!SingleLineBatch)  // OUTPUT("Optimization beginning time:\t\t%s (hr:min:sec.msec)\n",
                               // chrono::to_stream(now));
        {
          {
            if (!ForGlobalEpsPruning) {
              OutputFile << "Optimization beginning time:" << put_time(localtime(&timet), "%c %Z")
                         << "(hr:min : sec.msec)\n"
                         << endl;
            }
          }
        }
#endif

        Ssp->optimize();  // Later add an input condition so we can handle ORDER BY

#ifndef _TABLE_
        // OUTPUT elapsed time
        std::chrono::duration<double, std::milli> diff = std::chrono::system_clock::now() - now;
        if (!SingleLineBatch) {
          OUTPUT("Optimization elapsed time:\t\t" << (diff).count() << "ms");
          OUTPUT("========  OPTIMAL PLAN =========\n");
        }
#endif

        // CopyOut optimal plan. TRACE memory, search space.
        PHYS_PROP *PhysProp = CONT::vc[0]->GetPhysProp();
        /* CopyOut the Optimal plan starting from the RootGID (the root group of
        our query )
        */
        Ssp->CopyOut(Ssp->GetRootGID(), PhysProp, 0);
        PTRACE("---2--- memory statistics after optimization: " << DumpStatistics());
        if (TraceFinalSSP) {
          Ssp->FastDump();
        } else {
          PTRACE("final Search Space:" << endl << Ssp->Dump());
        }

        // Delete Contexts, close batch query file, delete search space
        if (!PiggyBack) {
          for (int i = 0; i < CONT::vc.size(); i++) delete CONT::vc[i];
          CONT::vc.clear();
        }
        // if (RadioVal ==0 && q==NumQuery-1) fclose(fp);
        // Go on with the usual procedure of deleting the search space before
        // reading in the next query of the batch query file if not in the
        // PiggyBack mode
        // else keep the search space for reuse
        if (!PiggyBack) delete Ssp;
        PTRACE("---3--- memory statistics after freeing searching space: " << DumpStatistics());

        // OUTPUT Rule Set Statistics
#ifndef _TABLE_
        if (!SingleLineBatch) OUTPUT(ruleSet->DumpStats());
#endif

#ifdef _TABLE_
      }
#endif

      // Report memory, delete catalog
      delete Cat;
    }                     // for each query
    if (RadioVal) break;  // If single query case, execute only once

    OUTPUT("\n");
    OUTPUT(" =============END OF A SEQUENCE================ ");
    OUTPUT("\n");

    // this will be executed only if in PiggyBack mode
    // to delete the search space one last time
    if (PiggyBack) {
      delete Ssp;
      for (int i = 0; i < CONT::vc.size(); i++) delete CONT::vc[i];
      CONT::vc.clear();
    }

  } while (!feof(fp));  // end of do loop  over each batch query sequence

  // Free optimization stat object, cost model, rule set, heuristic cost
  delete OptStat;
  delete costModel;
  delete ruleSet;
  delete HeuristicCost;


  OutputFile.close();
  OutputCOVE.close();
}

int main() { Optimizer(); }

// g++ .\test.cpp .\cm.cpp .\supp.cpp .\cat.cpp .\group.cpp .\expr.cpp .\item.cpp .\logop.cpp .\mexpr.cpp .\physop.cpp
// .\query.cpp .\rules.cpp .\ssp.cpp .\tasks.cpp .\main.cpp

// g++ .\cm.cpp .\supp.cpp .\cat.cpp .\group.cpp .\expr.cpp .\item.cpp .\logop.cpp .\mexpr.cpp .\physop.cpp .\query.cpp
// .\rules.cpp .\ssp.cpp .\tasks.cpp .\main.cpp

// g++ cm.cpp supp.cpp cat.cpp group.cpp expr.cpp item.cpp logop.cpp mexpr.cpp physop.cpp query.cpp rules.cpp ssp.cpp
// tasks.cpp main.cpp
