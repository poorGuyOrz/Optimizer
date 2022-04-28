
#include "../header/cat.h"
#include "../header/cm.h"
#include "../header/global.h"
#include "../header/physop.h"
#include "../header/stdafx.h"
#include "../header/tasks.h"

int main(int argc, char const *argv[]) {
  CostModel *cmx = new CostModel("../case/cost");
  cout << cmx->Dump() << endl;

  auto RuleSet = new RULE_SET("../case/ruleset");
  cout << RuleSet->Dump() << endl;

  Cost *HeuristicCost = new Cost(0);
  cout << HeuristicCost->Dump() << endl;

  auto Cat = new CAT("../case/catalog");
  cout << Cat->Dump() << endl;

  auto Query = new QUERY("../case/query");
  cout << Query->Dump() << endl;

  return 0;
}

// g++ -g  supp.cpp cat.cpp group.cpp item.cpp logop.cpp  physop.cpp query.cpp rules.cpp ssp.cpp tasks.cpp mainOptimizer.cpp -std=c++17