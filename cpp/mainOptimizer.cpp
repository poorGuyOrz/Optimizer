
#include "../header/cat.h"
#include "../header/cm.h"
#include "../header/global.h"
#include "../header/physop.h"

INT_ARRAY TopMatch;
INT_ARRAY Bindings;
INT_ARRAY Conditions;

int main(int argc, char const *argv[]) {
  OutputFile.open("../colout.txt");
  OutputCOVE.open("../script.cove");

  GlobepsPruning = false;
  ForGlobalEpsPruning = false;
  OptStat = new OPT_STAT;
  ruleSet = new RuleSet();
  // Initialize Rule Firing Statistics
  TopMatch.resize(ruleSet->RuleCount);
  Bindings.resize(ruleSet->RuleCount);
  Conditions.resize(ruleSet->RuleCount);  // 625
  for (int RuleNum = 0; RuleNum < ruleSet->RuleCount; RuleNum++) {
    TopMatch[RuleNum] = 0;
    Bindings[RuleNum] = 0;
    Conditions[RuleNum] = 0;
  }

  costModel = new CostModel("../case/cost");

  Cost *HeuristicCost = new Cost(0);

  Cat = new CAT("../case/catalog");
  cout << Cat->Dump() << endl;

  query = new Query("../case/query");
  cout << endl << query->Dump() << endl;

  Ssp = new SearchSpace;
  Ssp->Init();
  Ssp->FastDump();

  delete query;

  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  auto timet = chrono::system_clock::to_time_t(now);

  cout << "Optimization beginning time:" << put_time(localtime(&timet), "%c %Z") << "(hr:min : sec.msec)\n" << endl;

  Ssp->optimize();

  std::chrono::duration<double, std::milli> diff = std::chrono::system_clock::now() - now;
  cout << "Optimization elapsed time:" << (diff).count() << "ms" << endl;

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
  OUTPUT(ruleSet->DumpStats());

  OutputFile.close();
  OutputCOVE.close();
  return 0;
}