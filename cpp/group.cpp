/*
group.cpp -  implementation of class GROUP
        $Revision: 12 $
        Implements class GROUP as defined in ssp.h

        Columbia Optimizer Framework

        A Joint Research Project of Portland State University
           and the Oregon Graduate Institute
        Directed by Leonard Shapiro and David Maier
        Supported by NSF Grants IRI-9610013 and IRI-9619977


*/
#include "../header/ssp.h"
#include "../header/stdafx.h"

//##ModelId=3B0C086603C7
GROUP::GROUP(M_EXPR *MExpr)
    : GroupID(MExpr->GetGrpID()), FirstLogMExpr(MExpr), LastLogMExpr(MExpr), FirstPhysMExpr(NULL), LastPhysMExpr(NULL) {
  if (!ForGlobalEpsPruning) ClassStat[C_GROUP].New();

  init_state();

  // find the log prop
  int arity = MExpr->GetArity();
  LOG_PROP **InputProp = NULL;
  if (arity == 0) {
    LogProp = (MExpr->GetOp())->FindLogProp(InputProp);
  } else {
    InputProp = new LOG_PROP *[arity];
    GROUP *Group;
    for (int i = 0; i < arity; i++) {
      Group = Ssp->GetGroup(MExpr->GetInput(i));
      InputProp[i] = Group->LogProp;
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

  LowerBd = new COST(cost);

  /* if the operator is EQJOIN with m tables, estimate group size
     is 2^m*2.5. else it is zero */
  if (MExpr->GetOp()->GetName() == ("EQJOIN")) {
    int NumTables = this->EstimateNumTables(MExpr);
    EstiGrpSize = pow(2, NumTables) * 2.5;
  } else
    EstiGrpSize = 0;

  // the initial value is -1, meaning no winner has been found
  count = -1;
  if (COVETrace)  // New Group
  {
    char buffer[10];  // Holds input group ID
    CString temp;
    if (arity) {
      for (int i = 0; i < arity; i++) {
        sprintf(buffer, "%d", MExpr->GetInput(i));
        // (void)itoa(MExpr -> GetInput(i), buffer, 10);
        temp = temp + " " + buffer;
      }
    }
    temp += " ";

    CString os;
    os.Format("addGroup { %d %p \" %s  \"%s} %s",

              GroupID, (MExpr), MExpr->GetOp()->Dump(), temp, LogProp->DumpCOVE());

    OutputCOVE << (os) << endl;
  }
}

// free up memory
//##ModelId=3B0C086603C9
GROUP::~GROUP() {
  if (!ForGlobalEpsPruning) ClassStat[C_GROUP].Delete();

  delete LogProp;
  delete LowerBd;

  M_EXPR *mexpr = FirstLogMExpr;
  M_EXPR *next = mexpr;
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

#ifndef IRPROP
  for (int i = 0; i < Winners.size(); i++) delete Winners[i];
#endif
}

// estimate the number of tables in EQJOIN
//##ModelId=3B0C086701FC
int GROUP::EstimateNumTables(M_EXPR *MExpr) {
  int table_num;
  int total = 0;
  GROUP *Group;
  int arity = MExpr->GetArity();
  // if the input is EQJOIN, continue to count all the input
  for (int i = 0; i < arity; i++) {
    Group = Ssp->GetGroup(MExpr->GetInput(i));
    if (Group->GetFirstLogMExpr()->GetOp()->GetName() == ("EQJOIN")) {
      table_num = Group->EstimateNumTables(Group->GetFirstLogMExpr());
    } else
      table_num = 1;
    total += table_num;
  }
  return total;
}

//##ModelId=3B0C08670089
void GROUP::NewMExpr(M_EXPR *MExpr) {
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
    char buffer[10];  // Holds input group ID
    CString temp;
    int arity = MExpr->GetArity();
    if (arity) {
      for (int i = 0; i < arity; i++) {
        sprintf(buffer, "%d", MExpr->GetInput(i));
        // (void)itoa(MExpr -> GetInput(i), buffer, 10);
        temp = temp + " " + buffer;
      }
    }
    temp += " ";

    CString os;
    os.Format("addExp { %d %p \" %s  \"%s} %s",

              GroupID, (MExpr), MExpr->GetOp()->Dump(), temp, LogProp->DumpCOVE());

    OutputCOVE << (os) << endl;
  }
}

//##ModelId=3B0C08670044
void GROUP::set_optimized(bool is_optimized) {
  SET_TRACE Trace(true);

  if (is_optimized) {
    PTRACE("group " << GroupID << " is completed");
  }

  State.optimized = is_optimized;
}

//##ModelId=3B0C086700BC
void GROUP::ShrinkSubGroup() {
  for (M_EXPR *MExpr = FirstLogMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    for (int i = 0; i < MExpr->GetArity(); i++) Ssp->ShrinkGroup(MExpr->GetInput(i));
  }
}

// Delete a physical MExpr from a group, save memory
//##ModelId=3B0C086700BD
void GROUP::DeletePhysMExpr(M_EXPR *PhysMExpr) {
  M_EXPR *MExpr = FirstPhysMExpr;
  M_EXPR *next;
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

string GROUP::Dump() {
  string os;
  int Size = 0;
  M_EXPR *MExpr;

  os = "----- Group " + to_string(GroupID) + " : -----" + "\n";

  for (MExpr = FirstLogMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    os += MExpr->Dump();
    os += " ; ";
    Size++;
  }
  for (MExpr = FirstPhysMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    os += MExpr->Dump();
    os += " ; ";
    Size++;
  }
  os += "\n----- has " + to_string(Size) + " MExprs -----\n";

  // Print Winner's circle
  os += "Winners:\n";

#ifdef IRPROP
  os += M_WINNER::mc[GroupID]->Dump();
  os += "\n";
#else

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
#endif

  os += "LowerBound: " + LowerBd->Dump() + "\n";

  os += "log_prop: ";
  os += (*LogProp).Dump();

  return os;
}

void GROUP::FastDump() {
  int Size = 0;
  M_EXPR *MExpr;

  OutputFile << "----- Group " << GroupID << " : -----" << endl;

  for (MExpr = FirstLogMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    OutputFile << MExpr->Dump() << " ; ";
    Size++;
  }
  for (MExpr = FirstPhysMExpr; MExpr != NULL; MExpr = MExpr->GetNextMExpr()) {
    OutputFile << MExpr->Dump() << " ; ";
    Size++;
  }

  OutputFile << endl << "----- has " << Size << " MExprs -----" << endl;

  // Print Winner's circle
  OutputFile << "Winners:" << endl;

#ifdef IRPROP
  OutputFile << "\t" << M_WINNER::mc[GroupID]->Dump() << endl;
#else
  Size = Winners.size();
  PHYS_PROP *PhysProp;
  if (!Size) OutputFile << "\tNo Winners" << endl;
  for (int i = 0; i < Size; i++) {
    PhysProp = Winners[i]->GetPhysProp();
    OutputFile << "\t" << PhysProp->Dump();
    OutputFile << ", " << (Winners[i]->GetMPlan() ? Winners[i]->GetMPlan()->Dump() : "NULL");
    OutputFile << ", " << (Winners[i]->GetCost() ? Winners[i]->GetCost()->Dump() : "-1");
    OutputFile << ", " << (Winners[i]->GetDone() ? "Done" : "Not done") << endl;
  }
#endif

  OutputFile << "LowerBound: " << LowerBd->Dump() << endl;
  OutputFile << "log_prop: " << (*LogProp).Dump();
}
