// ssp.cpp -  implementation of class SearchSpace

#include "../header/stdafx.h"
#include "../header/tasks.h"

#define SHRINK_INERVAL 10000
#define MAX_AVAIL_MEM 40000000  // available memory bound to 50M

extern Cost GlobalEpsBound;

SearchSpace::SearchSpace() : NewGrpID(-1) {
  // initialize HashTbl to contain HashTableSize elements, each initially nullptr.
  HashTbl = new MExression *[HtblSize];  // 8192
  for (ub4 i = 0; i < HtblSize; i++) HashTbl[i] = nullptr;
}

void SearchSpace::Init() {
  Expression *Expr = query->GetEXPR();

  // create the initial search space
  RootGID = NEW_GRPID;
  MExression *MExpr = CopyIn(Expr, RootGID);

  InitGroupNum = NewGrpID;
  if (COVETrace)  // End Initializing Search Space
    OutputCOVE << "EndInit\n" << endl;
}

// free up memory
SearchSpace::~SearchSpace() {
  for (int i = 0; i < Groups.size(); i++) delete Groups[i];
  delete[] HashTbl;
}

string SearchSpace::DumpHashTable() {
  string os;

  os = "Hash Table BEGIN:\n";
  int total = 0;
  for (int i = 0; i < HtblSize; i++) {
    for (MExression *mexpr = HashTbl[i]; mexpr != nullptr; mexpr = mexpr->GetNextHash(), total++)
      os += "group:" + to_string(mexpr->GetGrpID()) + " " + mexpr->Dump();
    if (HashTbl[i]) os += "\n";
  }
  os += "Hash Table END, total " + to_string(total) + " mexpr\n";

  return os;
}

string SearchSpace::DumpChanged() {
  string os;
  Group *group;

  for (int i = 0; i < Groups.size(); i++)
    if (Groups[i]->is_changed()) {
      group = Groups[i];
      os += group->Dump();
      group->set_changed(false);
    }

  if (os != "")
    return ("Changed Search Space:\n" + os);
  else
    return ("Search Space not changed");
}

void SearchSpace::Shrink() {
  for (int i = InitGroupNum; i < Groups.size(); i++) ShrinkGroup(i);
}

void SearchSpace::ShrinkGroup(int group_no) {
  Group *group;
  MExression *mexpr;
  MExression *p;
  MExression *prev;
  int DeleteCount = 0;

  SET_TRACE Trace(true);

  group = Groups[group_no];

  if (!group->is_optimized() && !group->is_explored()) return;  // may be pruned

  PTRACE("Shrinking group " << group_no << ",");

  // Shrink the logical mexpr
  // init the rule mark of the first mexpr to 0, means all rules are allowed
  mexpr = group->GetFirstLogMExpr();
  mexpr->set_rule_mask(0);

  // delete all the mexpr except the first initial one
  mexpr = mexpr->GetNextMExpr();

  while (mexpr != nullptr) {
    // maintain the hash link
    // find my self in the appropriate hash bucket
    ub4 hashval = mexpr->hash();
    for (p = HashTbl[hashval], prev = nullptr; p != mexpr; prev = p, p = p->GetNextHash())
      ;

    assert(p == mexpr);
    // link prev's next hash to next
    if (prev)
      prev->SetNextHash(mexpr->GetNextHash());
    else
      // the mexpr is the first in the bucket
      HashTbl[hashval] = mexpr->GetNextHash();

    p = mexpr;
    mexpr = mexpr->GetNextMExpr();

    delete p;
    DeleteCount++;
  }

  mexpr = group->GetFirstLogMExpr();
  mexpr->SetNextMExpr(nullptr);
  // update the lastlogmexpr = firstlogmexpr;
  group->SetLastLogMExpr(mexpr);

  // Shrink the physcal mexpr
  mexpr = group->GetFirstPhysMExpr();

  while (mexpr != nullptr) {
    p = mexpr;
    mexpr = mexpr->GetNextMExpr();

    delete p;
    DeleteCount++;
  }

  mexpr = group->GetFirstPhysMExpr();
  mexpr->SetNextMExpr(nullptr);
  // update the lastlogmexpr = firstlogmexpr;
  group->SetLastPhysMExpr(mexpr);

  group->set_changed(true);
  group->set_exploring(false);

  PTRACE("Deleted " << DeleteCount << " mexpr!\n");
}

string SearchSpace::Dump() {
  string os;
  Group *group;

  os = "RootGID:" + to_string(RootGID) + "\n";

  for (int i = 0; i < Groups.size(); i++) {
    group = Groups[i];
    os += group->Dump();
    group->set_changed(false);
  }

  return os;
}

void SearchSpace::FastDump() {
  OutputFile << "SearchSpace Content: RootGID: " << RootGID << endl;

  for (int i = 0; i < Groups.size(); i++) {
    Groups[i]->FastDump();
    Groups[i]->set_changed(false);
  }
}

MExression *SearchSpace::FindDup(MExression &MExpr) {
  int Arity = MExpr.GetArity();

  ub4 hashval = MExpr.hash();
  MExression *prev = HashTbl[hashval];

  int BucketSize = 0;
  if (!ForGlobalEpsPruning) OptStat->HashedMExpr++;
  // try all expressions in the appropriate hash bucket
  for (MExression *old = prev; old != nullptr; prev = old, old = old->GetNextHash(), BucketSize++) {
    int input_no;

    // See if they have the same arities
    if (old->GetArity() != Arity) {
      goto not_a_duplicate;
    }

    // finding yourself does not constitute a duplicate
    // compare pointers to see if EXPR_LISTs are the same
    if (old == &MExpr) {
      goto not_a_duplicate;
    }

    // compare the inputs
    // Compare the actual group pointers for every input
    for (input_no = Arity; --input_no >= 0;)
      if (MExpr.GetInput(input_no) != old->GetInput(input_no)) {
        PTRACE("Different at input " << input_no);
        goto not_a_duplicate;
      }

    // finally compare the Op
    if (!(*(old->GetOp()) == MExpr.GetOp())) {
      PTRACE("Different at Operator. " << old->Dump() << " : " << MExpr.Dump());
      goto not_a_duplicate;
    }

    // "expr" is a duplicate of "old"
    return (old);

  not_a_duplicate:
    continue;  // check next expression in hash bucket
  }            // try all expressions in the appropriate hash bucket

  // no duplicate, insert into HashTable
  if (prev == nullptr)
    HashTbl[hashval] = &MExpr;
  else
    prev->SetNextHash(&MExpr);

  if (!ForGlobalEpsPruning) {
    if (OptStat->MaxBucket < BucketSize) OptStat->MaxBucket = BucketSize;
  }

  return (nullptr);
}  // SearchSpace::FindDup

// merge two groups when duplicate found in these two groups
// means they should be the same group
// always merge bigger group_no group to smaller one.

int SearchSpace::MergeGroups(int group_no1, int group_no2) {
  // MExression * mexpr;

  int ToGid = group_no1;
  int FromGid = group_no2;

  // always merge bigger group_no group to smaller one.
  if (group_no1 > group_no2) {
    ToGid = group_no2;
    FromGid = group_no1;
  }

#ifdef UNIQ
  assert(false);
#endif

  return ToGid;
}  // SearchSpace::MergeGroups

MExression *SearchSpace::CopyIn(Expression *Expr, int &GrpID) {
  Group *group;
  bool win = true;  // will we initialize nontrivial winners in this group?
  // False if it is a subgroup of a DUMMY operator

  // Factor GrpID value into normal value plus win value
  if (GrpID == NEW_GRPID_NOWIN) {
    GrpID = NEW_GRPID;
#ifdef DUMNOWIN
    win = false;
#endif
  }

  // create the M_Expr which will reside in the group
  MExression *MExpr = new MExression(Expr, GrpID);

  // find duplicate.  Done only for logical, not physical, expressions.
  if (MExpr->GetOp()->is_logical()) {
    MExression *DupMExpr = FindDup(*MExpr);
    if (DupMExpr != nullptr)  // not null ,there is a duplicate
    {
      if (!ForGlobalEpsPruning) OptStat->DupMExpr++;  // calculate dup mexpr
      PTRACE("duplicate mexpr : " << MExpr->Dump());

      // the duplicate is in the group the expr wanted to copyin
      if (GrpID == DupMExpr->GetGrpID()) {
        delete MExpr;
        return nullptr;
      }

      // If the Mexpr is supposed to be in a new group, set the group id
      if (GrpID == NEW_GRPID) {
        GrpID = DupMExpr->GetGrpID();

        // because the NewGrpID increases when constructing
        // an MExression with NEW_GRPID, we need to decrease it
        NewGrpID--;

        delete MExpr;
        return nullptr;
      } else {
        // otherwise, i.e., GrpID != DupMExpr->GrpID
        // need do the merge
        GrpID = MergeGroups(GrpID, DupMExpr->GetGrpID());
        delete MExpr;
        return nullptr;
      }
    }  // if( DupMExpr != nullptr )
  }    // If the expression is logical

  // no duplicate found
  if (GrpID == NEW_GRPID) {
    // create a new group
    group = new Group(MExpr);

    // insert the new group into ssp
    GrpID = group->GetGroupID();

    if (GrpID >= Groups.size()) Groups.resize(GrpID + 1);
    Groups[GrpID] = group;

  } else {
    group = GetGroup(GrpID);

    // include the new MEXPR
    group->NewMExpr(MExpr);
  }
  // set the flag
  group->set_changed(true);

  return MExpr;
}  // SearchSpace::CopyIn

void SearchSpace::CopyOut(int GrpID, PHYS_PROP *PhysProp, int tabs) {
  // Find the winner for this Physical Property.
  // print the Winner's Operator and cost
  Group *ThisGroup = Ssp->GetGroup(GrpID);

  WINNER *ThisWinner;

  MExression *WinnerMExpr;
  Operator *WinnerOp;
  string os;

  // special case : it's a const group
  if (ThisGroup->GetFirstLogMExpr()->GetOp()->is_const()) {
    WinnerMExpr = ThisGroup->GetFirstLogMExpr();
    os = WinnerMExpr->Dump();
    os += ", Cost = 0\n";
    OUTPUTN(tabs, os);
  }

  // It's an item group
  else if (ThisGroup->GetFirstLogMExpr()->GetOp()->is_item()) {
    ThisWinner = ThisGroup->GetWinner(PhysProp);

    if (ThisWinner == nullptr) {
      os = "No optimal plan for group: " + to_string(GrpID) + " with phys_prop: " + PhysProp->Dump() + "\n";
      OUTPUTN(tabs, os);
      return;
    }

    assert(ThisWinner->GetDone());
    WinnerMExpr = ThisWinner->GetMPlan();
    WinnerOp = WinnerMExpr->GetOp();

    os = WinnerMExpr->Dump();
    os += ", Cost = ";

    OUTPUTN(tabs, os);

    Cost *WinnerCost = ThisWinner->GetCost();
    os = WinnerCost->Dump();

    OUTPUT(os);
    PHYS_PROP *InputProp;
    // print the input recursively
    for (int i = 0; i < WinnerMExpr->GetArity(); i++) {
      InputProp = new PHYS_PROP(any);
      CopyOut(WinnerMExpr->GetInput(i), InputProp, tabs + 1);
      delete InputProp;
    }
  }
  // it's a normal group
  else {
    // First extract the winning expression for this property
    ThisWinner = ThisGroup->GetWinner(PhysProp);

    if (ThisWinner == nullptr) {
      os = "No optimal plan for group: " + to_string(GrpID) + " with phys_prop: " + PhysProp->Dump() + "\n";
      OUTPUTN(tabs, os);
      return;
    }

    assert(ThisWinner->GetDone());
    WinnerMExpr = ThisWinner->GetMPlan();

    // Now extract the operator from the expression and write it to the output string
    //  along with   " cost = " .  Print output string to window.
    assert(WinnerMExpr != nullptr);

    WinnerOp = WinnerMExpr->GetOp();
    os = WinnerMExpr->Dump();
    if (WinnerOp->GetName() == "QSORT") os += PhysProp->Dump();
    os += ", Cost = ";

    if (!SingleLineBatch) OUTPUTN(tabs, os);

    // Extract cost of the winner, write it to the output string and
    //  print output string to window.
    Cost *WinnerCost = ThisWinner->GetCost();
    os = WinnerCost->Dump();

    OUTPUT(os);
    if (SingleLineBatch)  // In this case we want only the total cost of the Winner
    {
      OUTPUT("\n");
      return;
    }

    // Recursively print inputs
    int Arity = WinnerOp->GetArity();
    PHYS_PROP *ReqProp;
    bool possible;
    for (int i = 0; i < Arity; i++) {
      int input_groupno = WinnerMExpr->GetInput(i);

      ReqProp = ((PhysicalOperator *)WinnerOp)
                    ->InputReqdProp(PhysProp, Ssp->GetGroup(input_groupno)->get_log_prop(), i, possible);

      assert(possible);  // Otherwise optimization fails

      CopyOut(input_groupno, ReqProp, tabs + 1);

      delete ReqProp;
    }
  }
}  // SearchSpace::CopyOut()

bool Group::firstplan = false;

/* bool Group::search_circle(CONT * C, bool & moresearch)
    {
            First search for a winner with property P.
            If there is no such winner, case (3)
            If there is a winner, denote its plan component by WPlan and
                    its cost component by WCost.
                    Context cost component is CCost

            If (WPlan is non-null) //Cheapest plan costs *WCost;
                     //we seek a plan costing *CCost or less
                    If (*WCost <= *CCost)
                            Case (2)
                    else if (*CCost < *WCost)
                            Case (1)
            else If (WPlan is null) //All plans cost more than *WCost
                    if( *CCost <= *WCost)
                            Case (1)
                    else if (*WCost < *CCost) //There might be a plan between WCost and CCost
                            Case (4)
*/

/* Group::search_circle
Map between four cases (see header file) and the way they arise:

No winner for this property:  (3)

                                WCost >= CCost	WCost < CCost	WCost <= CCost	WCost > CCost

MPlan is Null			(1)			  (4)

MPlan not Null										  (2)			  (1)

*/
bool Group::search_circle(CONT *C, bool &moreSearch) {
  // First search for a winner with property P.
  WINNER *Winner = GetWinner(C->GetPhysProp());

  // If there is no such winner, case (3)
  if (!Winner) {
    moreSearch = true;
    return (false);
  }

  assert(Winner->GetDone());  // This is not a recursive query

  // If there is a winner, denote its plan, cost components by M and WCost
  // Context cost component is CCost
  MExression *M = Winner->GetMPlan();
  Cost *WCost = Winner->GetCost();
  Cost *CCost = C->GetUpperBd();
  assert(CCost);  // Did we get rid of all cruft?

  if (M)  // there is a non-null winner
  {
    if (*CCost >= *WCost)  // Real winner; CCost is less of a constraint.  Case (2)
    {
      moreSearch = false;
      return (true);
    } else  // search is impossible as winner's cost is more than required context cost (1)
    {
      moreSearch = false;
      return (false);
    }
  } else  // Winner's Mplan is null.
  {
    if (*WCost >= *CCost)  // Previous search failed and CCost is more of a constraint. (1)
    {
      moreSearch = false;
      return (false);
    } else  // Previous search failed but CCost is less of a constraint.  (4)
    {
      moreSearch = true;
      return (true);
    }
  }
}

WINNER *Group::GetWinner(PHYS_PROP *PhysProp) {
  int Size = Winners.size();
  for (int i = 0; i < Size; i++) {
    PHYS_PROP *WinPhysProp = Winners[i]->GetPhysProp();

    if (*WinPhysProp == *PhysProp) return (Winners[i]);
  }

  // No matching winner
  return (nullptr);
}

void Group::NewWinner(PHYS_PROP *ReqdProp, MExression *MExpr, Cost *TotalCost, bool done) {
  if (COVETrace && MExpr)  // New Winner
  {
    OutputCOVE << "NewWin " << to_string(MExpr->GetGrpID()) << " \"" << ReqdProp->Dump() << "\"" << TotalCost->Dump()
               << "  { " << to_string(MExpr->GetGrpID()) << " " << MExpr << " \"" << MExpr->Dump() << "\" "
               << (done ? "Done" : "Not Done") << " }\n"
               << endl;
  }

  this->set_changed(true);

  // Seek winner with property ReqdProp in the winner's circle
  for (int i = Winners.size(); --i >= 0;) {
    if (*(Winners[i]->GetPhysProp()) == *ReqdProp) {
      // Update the winner for the new search just begun
      delete Winners[i];
      Winners[i] = new WINNER(MExpr, ReqdProp, TotalCost, done);
      return;
    }
  }

  // No matching winner for this property
  Winners.push_back(new WINNER(MExpr, ReqdProp, TotalCost, done));

  return;
}

bool Group::CheckWinnerDone() {
  // Search Winner's circle.  If there is a winner done, return true

  int Size = Winners.size();

  for (int i = 0; i < Size; i++) {
    if (Winners[i]->GetDone()) return (true);
  }

  // No winner is done
  return (false);
}  // Group::CheckWinnerDone

WINNER::WINNER(MExression *MExpr, PHYS_PROP *PhysProp, Cost *cost, bool done)
    : cost(cost), MPlan((MExpr == nullptr) ? nullptr : (new MExression(*MExpr))), PhysProp(PhysProp), Done(done){};

int TaskNo;
int Memo_M_Exprs;

void SearchSpace::optimize() {
  Ssp->GetGroup(0)->setfirstplan(false);

  // Create initial context, with no requested properties, infinite upper bound,
  //  zero lower bound, not yet done.  Later this may be specified by user.
  if (CONT::vc.size() == 0) {
    CONT *InitCont = new CONT(new PHYS_PROP(any), new Cost(-1), false);
    // Make this the first context
    CONT::vc.push_back(InitCont);
  }

  // start optimization with root group, 0th context, parent task of zero.
  if (GlobepsPruning) {
    Cost *eps_bound = new Cost(GlobalEpsBound);
    PTasks.push(new OptimizeGroupTask(RootGID, 0, 0, true, eps_bound));
  } else
    PTasks.push(new OptimizeGroupTask(RootGID, 0, 0, true, nullptr));

  while (!PTasks.empty()) {
    TaskNo++;
    PTRACE("--------------------------Starting task " << TaskNo << "-----------------------------");

    OptimizerTask *task = PTasks.pop();
    task->perform();

    PTRACE("------------------ SearchSpace after task " << TaskNo << ": ");
    OutputFile << DumpChanged() << endl;

    PTRACE("------------------ OPEN after task " << TaskNo << ":");
    OutputFile << PTasks.Dump() << endl;

    OutputFile << endl << DumpHashTable() << endl;
  }

  PTRACE("Optimizing completed: " << TaskNo << " tasks\n");
  OUTPUT("TotalTask : " << TaskNo);
  OUTPUT("TotalMExpr in MEMO: " << Memo_M_Exprs);
  OUTPUT(OptStat->Dump());
}
