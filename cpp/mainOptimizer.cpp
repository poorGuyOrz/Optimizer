
#include "../header/cat.h"
#include "../header/cm.h"
#include "../header/global.h"
#include "../header/physop.h"
#include "../header/stdafx.h"
#include "../header/tasks.h"

int main(int argc, char const *argv[]) {
  OutputFile.open("../colout.txt");
  OutputCOVE.open("../script.cove");
  GlobepsPruning = false;
  ForGlobalEpsPruning = false;
  OptStat = new OPT_STAT;

  costModel = new CostModel("../case/cost");

  ruleSet = new RuleSet();
  cout << ruleSet->Dump() << endl;

  Cost *HeuristicCost = new Cost(0);
  cout << HeuristicCost->Dump() << endl;

  Cat = new CAT("../case/catalog");
  cout << Cat->Dump() << endl;

  query = new Query("../case/query");
  cout << endl << query->Dump() << endl;

  Ssp = new SearchSpace;
  Ssp->Init();
  Ssp->FastDump();

  cout << "ssp-->" << endl << Ssp->DumpHashTable() << endl;
  delete query;

  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  auto timet = chrono::system_clock::to_time_t(now);

  cout << "Optimization beginning time:" << put_time(localtime(&timet), "%c %Z") << "(hr:min : sec.msec)\n" << endl;

  Ssp->optimize();

  std::chrono::duration<double, std::milli> diff = std::chrono::system_clock::now() - now;
  cout << "Optimization elapsed time:" << (diff).count() << "ms" << endl;
  cout << "ssp-->" << endl << Ssp->DumpHashTable() << endl;

  PHYS_PROP *PhysProp = CONT::vc[0]->GetPhysProp();
  Ssp->CopyOut(Ssp->GetRootGID(), PhysProp, 0);
  *HeuristicCost = *(Ssp->GetGroup(0)->GetWinner(PhysProp)->GetCost());
  assert(Ssp->GetGroup(0)->GetWinner(PhysProp)->GetDone());
  GlobalEpsBound = (*HeuristicCost) * (GLOBAL_EPS);
  delete Ssp;
  for (int i = 0; i < CONT::vc.size(); i++) delete CONT::vc[i];
  CONT::vc.clear();
  delete Cat;
  GlobepsPruning = true;
  ForGlobalEpsPruning = false;

  OutputFile.close();
  OutputCOVE.close();
  return 0;
}

// g++ -g  supp.cpp cat.cpp group.cpp item.cpp logop.cpp  physop.cpp query.cpp rules.cpp ssp.cpp tasks.cpp
// mainOptimizer.cpp -std=c++17