// group.cpp -  implementation of class Group
#include "../header/ssp.h"
#include "../header/stdafx.h"

Group::Group(MExression *MExpr)
    : GroupID(MExpr->GetGrpID()), FirstLogMExpr(MExpr), LastLogMExpr(MExpr), FirstPhysMExpr(NULL), LastPhysMExpr(NULL) {
  init_state();

  // find the log prop
  int arity = MExpr->GetArity();
  LOG_PROP **InputProp = NULL;
  if (arity == 0) {
    LogProp = (MExpr->GetOp())->FindLogProp(InputProp);
  } else {
    InputProp = new LOG_PROP *[arity];
    Group *group;
    for (int i = 0; i < arity; i++) {
      group = Ssp->GetGroup(MExpr->GetInput(i));
      InputProp[i] = group->LogProp;
    }

    LogProp = ((MExpr->GetOp())->FindLogProp(InputProp));

    delete[] InputProp;
  }

  /* Calculate the LowerBd, which is:
  TouchCopyCost:
  touchcopy() * |G| +     //From top join
  touchcopy() * sum(cucard(Ai) i = 2, ..., n-1) +  // from other, nontop, joins
  + FetchCost:
  fetchbound() * sum(cucard(Ai) i = 1, ..., n) // from leaf fetches
  */
  double cost = 0;
  if (MExpr->GetOp()->is_logical()) {
    if (MExpr->GetOp()->GetName() == ("GET"))
      cost = 0;  // Since GET operator does not have a CopyOut cost
    else
      cost = TouchCopyCost((LOG_COLL_PROP *)LogProp);

    // Add in fetching cost if CuCard Pruning
    if (CuCardPruning) cost += FetchingCost((LOG_COLL_PROP *)LogProp);
  }

  LowerBd = new Cost(cost);

  /* if the operator is EQJOIN with m tables, estimate group size
     is 2^m*2.5. else it is zero */
  if (MExpr->GetOp()->GetName() == ("EQJOIN")) {
    int NumTables = this->EstimateNumTables(MExpr);
    EstiGrpSize = pow(2, NumTables) * 2.5;
  } else
    EstiGrpSize = 0;

  // the initial value is -1, meaning no winner has been found
  count = -1;
  if (COVETrace) {
    string os;
    if (arity) {
      for (int i = 0; i < arity; i++) {
        // (void)itoa(MExpr -> GetInput(i), buffer, 10);
        os = os + " " + to_string(MExpr->GetInput(i));
      }
    }

    OutputCOVE << "\taddGroup { " << GroupID << " " << MExpr << " [ " << MExpr->GetOp()->Dump() << "  " << os << " ]} "
               << LogProp->DumpCOVE() << endl;
  }
}

// free up memory
Group::~Group() {
  delete LogProp;
  delete LowerBd;

  MExression *mexpr = FirstLogMExpr;
  MExression *next = mexpr;
  while (next != NULL) {
    next = mexpr->GetNextMExpr();
    delete mexpr;
    mexpr = next;
  }

  mexpr = FirstPhysMExpr;
  next = mexpr;
  while (next != NULL) {
    next = mexpr->GetNextMExpr();
    delete mexpr;
    mexpr = next;
  }

  for (int i = 0; i < Winners.size(); i++) delete Winners[i];
}

// estimate the number of tables in EQJOIN
//##ModelId=3B0C086701FC
int Group::EstimateNumTables(MExression *MExpr) {
  int table_num;
  int total = 0;
  Group *group;
  int arity = MExpr->GetArity();
  // if the input is EQJOIN, continue to count all the input
  for (int i = 0; i < arity; i++) {
    group = Ssp->GetGroup(MExpr->GetInput(i));
    if (group->GetFirstLogMExpr()->GetOp()->GetName() == ("EQJOIN")) {
      table_num = group->EstimateNumTables(group->GetFirstLogMExpr());
    } else
      table_num = 1;
    total += table_num;
  }
  return total;
}

void Group::NewMExpr(MExression *MExpr) {
  // link to last mexpr
  if (MExpr->GetOp()->is_logical()) {
    LastLogMExpr->SetNextMExpr(MExpr);
    LastLogMExpr = MExpr;
  } else {
    if (LastPhysMExpr)
      LastPhysMExpr->SetNextMExpr(MExpr);
    else
      FirstPhysMExpr = MExpr;

    LastPhysMExpr = MExpr;
  }

  // if there is a winner found before, count the number of plans
  if (count != -1) count++;
  if (COVETrace)  // New MExpr
  {
    string os;
    int arity = MExpr->GetArity();
    if (arity) {
      for (int i = 0; i < arity; i++) {
        // (void)itoa(MExpr -> GetInput(i), buffer, 10);
        os = os + " " + to_string(MExpr->GetInput(i));
      }
    }
    os += " ";

    OutputCOVE << "\taddExp { " << GroupID << " " << MExpr << " [ " << MExpr->GetOp()->Dump() << " " << os << " ]} "
               << LogProp->DumpCOVE() << endl;
  }
}

void Group::set_optimized(bool is_optimized) {
  if (is_optimized) {
    PTRACE("group " << GroupID << " is completed");
  }

  State.optimized = is_optimized;
}

void Group::ShrinkSubGroup() {
  for (MExression *MExpr = FirstLogMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    for (int i = 0; i < MExpr->GetArity(); i++) Ssp->ShrinkGroup(MExpr->GetInput(i));
  }
}

// Delete a physical MExpr from a group, save memory
void Group::DeletePhysMExpr(MExression *PhysMExpr) {
  MExression *MExpr = FirstPhysMExpr;
  MExression *next;
  if (MExpr == PhysMExpr) {
    FirstPhysMExpr = MExpr->GetNextMExpr();
    // if the MExpr to be deleted is the only one in the group,
    // set both First and Last Physical MExpr to be NULL
    if (FirstPhysMExpr == NULL) LastPhysMExpr = NULL;
    delete MExpr;
    MExpr = NULL;
  } else {
    // search for the MExpr to be deleted in the link list
    while (MExpr != NULL) {
      next = MExpr->GetNextMExpr();
      // if found, manipulate the pointers and delete the necessary one
      if (next == PhysMExpr) {
        MExpr->SetNextMExpr(next->GetNextMExpr());
        if (next->GetNextMExpr() == NULL) LastPhysMExpr = MExpr;
        delete next;
        next = NULL;
        break;
      }
      MExpr = MExpr->GetNextMExpr();
    }
  }
}

string Group::Dump(bool dumpexpr) {
  string os;
  int Size = 0;
  MExression *MExpr;

  if (!dumpexpr) os = "Group: " + to_string(GroupID) + "\n";

  for (MExpr = FirstLogMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    os += "\tLogic M_Expr: ";
    os += MExpr->Dump();
    os += ";\n";
    Size++;
  }
  for (MExpr = FirstPhysMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    os += "\tPhysc M_Expr: ";
    os += MExpr->Dump();
    os += ";\n";
    Size++;
  }
  os += "\t----- has " + to_string(Size) + " MExprs -----\n";
  if (dumpexpr) return os;

  // Print Winner's circle
  os += "Winners:\n";

  Size = Winners.size();
  PHYS_PROP *PhysProp;
  if (!Size) os += "\tNo Winners\n";
  for (int i = 0; i < Size; i++) {
    PhysProp = Winners[i]->GetPhysProp();
    os += "\t";
    os += PhysProp->Dump();
    os += ", ";
    os += (Winners[i]->GetMPlan() ? Winners[i]->GetMPlan()->Dump() : "NULL Plan");
    os += ", ";
    os += (Winners[i]->GetCost() ? Winners[i]->GetCost()->Dump() : "NULL Cost");
    os += ", ";
    os += (Winners[i]->GetDone() ? "Done" : "Not done");
    os += "\n";
  }
  os += "LowerBound: " + LowerBd->Dump() + "\n";

  os += "log_prop: ";
  os += (*LogProp).Dump();
  os += "\n";

  return os;
}
