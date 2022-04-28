// SSP.H - Search Space

#pragma once
#include "../header/query.h"

#define NEW_GRPID -1  // used by SSP::CopyIn and M_EXPR::M_EXPR
// means need to create a new group
#define NEW_GRPID_NOWIN -2  // use by SSP::CopyIn.  Means NEW_GRPID, and this is a
// subgroup of DUMMY so don't init nontrivial winners
class SSP;
class GROUP;
class M_EXPR;
class WINNER;
class M_WINNER;

/*
============================================================
SEARCH SPACE - class SSP
============================================================
We borrow the term Search Space from AI, where it is a tool for solving a
problem  In query optimization the problem is to find the cheapest plan
for a given query subject to certain context (class CONT).

A Search Space typically consists of a collection of possible solutions
to the problem and its subproblems. Dynamic Programming and
Memoization are two approaches to using a Search Space to solve a problem.
Both Dynamic Programming and Memoization partition the possible solutions
by logical equivalence. We will call each partition a GROUP.  Thus a GROUP
contains a collection of logically equivalent expressions.

In our setting of query optimization, logical equivalence is query equivalence.
Each GROUP corresponds to a temporary collection, or a subquery, in a
computation of the main query.  If the main query is A join B join C, then
there are GROUPs representing A join B, A join C, C, etc.

In our approach to query optimization, each possible solution in the Search
Space is represented compactly, by what we call a Multi-expression
(class M_EXPR).  A solution in the search space can also be represented in
more detail by an expression (class EXPR).

*/
class SSP  // Search Space
{
 public:
  M_EXPR **HashTbl;  // To identify duplicate MExprs

  SSP();

  void Init();  // Create some default number of empty Groups, the number
                // depending on the initial query.  Read in initial query.

  ~SSP();

  void optimize();  // Later add a conditon.
                    //  Prepare the SSP so an optimal plan can be found

  // Convert the EXPR into a Mexpr.
  // If Mexpr is not already in the search space, then copy Mexpr into the
  // search space and return the new Mexpr.
  // If Mexpr is already in the search space, then either throw it away or
  // merge groups.
  // GrpID is the ID of the group where Mexpr will be put.  If GrpID is
  // NEW_GRPID(-1), make a new group with that ID and return its value in GrpID.
  M_EXPR *CopyIn(EXPR *Expr, GRP_ID &GrpID);

  // Copy out the final plan.  Recursive, each time increasing tabs by
  //  one, so the plan is indented.
  //##ModelId=3B0C0865007C
  void CopyOut(GRP_ID GrpID, PHYS_PROP *PhysProp, int tabs);

  // return the next available grpID in SSP
  //##ModelId=3B0C08650087
  inline GRP_ID GetNewGrpID() { return (++NewGrpID); };

  // return the ID of the Root group
  //##ModelId=3B0C08650090
  inline GRP_ID GetRootGID() { return (RootGID); };

  // return the specific group
  //##ModelId=3B0C0865009A
  inline GROUP *GetGroup(GRP_ID Gid) { return Groups[Gid]; };

  // If another expression in the search space is identical to MExpr, return
  //  it, else return NULL.
  //  Identical means operators and arguments, and input groups are the same.
  //##ModelId=3B0C086500A5
  M_EXPR *FindDup(M_EXPR &MExpr);

  // When a duplicate is found in two groups they should be merged into
  //  the same group.  We always merge bigger group_no group to smaller one.
  //##ModelId=3B0C086500AE
  GRP_ID MergeGroups(GRP_ID group_no1, GRP_ID group_no2);
  // GRP_ID MergeGroups(GROUP & ToGroup, GROUP & FromGroup);

  //##ModelId=3B0C086500B9
  void ShrinkGroup(GRP_ID group_no);  // shrink the group marked completed
  //##ModelId=3B0C086500C3
  void Shrink();  // shrink the ssp

  //##ModelId=3B0C086500CD
  bool IsChanged();  // is the ssp changed?

  string Dump();
  void FastDump();

  string DumpChanged();
  string DumpHashTable();

 private:
  //##ModelId=3B0C086500F6
  GRP_ID RootGID;  // ID of the oldest, root group.  Set to NEW_GRPID in SSP""Init then
  // never changes
  //##ModelId=3B0C08650114
  GRP_ID InitGroupNum;  //(seems to be the same as RootGID)
  //##ModelId=3B0C08650128
  GRP_ID NewGrpID;  // ID of the newest group, initially -1

  // Collection of Groups, indexed by GRP_ID
  //##ModelId=3B0C0865013C
  vector<GROUP *> Groups;

};  // class SSP

/*
   ============================================================
   MULTI_EXPRESSIONS - class M_EXP
   ============================================================
   M_EXPR is a compact form of EXPR which utilizes sharing.  Inputs are
   GROUPs instead of EXPRs, so the M_EXPR embodies several EXPRs.
   All searching is done with M_EXPRs, so each contains lots of state.

*/
class M_EXPR {
 private:
  M_EXPR *HashPtr;  // list within hash bucket

  BIT_VECTOR RuleMask;  // If 1, do not fire rule with that index

  int counter;  // to keep track of how many winners point to this MEXPR
  OP *Op;       // Operator
  GRP_ID *Inputs;
  GRP_ID GrpID;  // I reside in this group

  // link to the next mexpr in the same group
  M_EXPR *NextMExpr;

  /*
  //This struct will be replaced with more efficient and flexible storage
  //Histor of which rules have been fired on this M_EXPR
  struct RuleHist {
  } RuleHist;
  */
 public:
  /*M_EXPR(OP * Op,
    GRP_ID First = NULL, GRP_ID Second = NULL,
    GRP_ID Third = NULL, GRP_ID Fourth = NULL);
  */
  ~M_EXPR() {
    if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR].Delete();
    if (GetArity()) {
      delete[] Inputs;
    }

    delete Op;
    Op = NULL;
  };

  // M_EXPR(OP * Op, GRP_ID* inputs); //Used by CopyIn

  // Transform an EXPR into an M_EXPR.  May involve creating new Groups.
  //  GrpID is the ID of the group where the M_EXPR will be put.  If GrpID is
  //  NEW_GRPID(-1), make a new group with that ID.  (Same as SSP::CopyIn)
  M_EXPR(EXPR *Expr, GRP_ID grpid)
      : Op(Expr->GetOp()->Clone()),
        NextMExpr(NULL),
        GrpID((grpid == NEW_GRPID) ? Ssp->GetNewGrpID() : grpid),
        HashPtr(NULL),
        RuleMask(0) {
    GRP_ID GID;
    EXPR *input;
    counter = 0;

    if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR].New();

    // copy in the sub-expression
    int arity = GetArity();
    if (arity) {
      Inputs = new GRP_ID[arity];
      for (int i = 0; i < arity; i++) {
        input = Expr->GetInput(i);

        if (input->GetOp()->is_leaf())  // deal with LEAF_OP, sharing the existing group
          GID = ((LEAF_OP *)input->GetOp())->GetGroup();
        else {
          // create a new sub group
          if (Op->GetName() == "DUMMY")
            GID = NEW_GRPID_NOWIN;  // DUMMY subgroups have only trivial winners
          else
            GID = NEW_GRPID;
          M_EXPR *MExpr = Ssp->CopyIn(input, GID);
        }

        Inputs[i] = GID;
      }
    }  // if(arity)
  };

  M_EXPR(M_EXPR &other)
      : GrpID(other.GrpID),
        HashPtr(other.HashPtr),
        NextMExpr(other.NextMExpr),
        Op(other.Op->Clone()),
        RuleMask(other.RuleMask) {
    if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR].New();

    // Inputs are the only member data left to copy.
    int arity = Op->GetArity();
    if (arity) {
      Inputs = new GRP_ID[arity];
      while (--arity >= 0) Inputs[arity] = other.GetInput(arity);
    }
  };

  inline int GetCounter() { return counter; };
  inline void IncCounter() { counter++; };
  inline void DecCounter() {
    if (counter != 0) counter--;
  };
  inline OP *GetOp() { return (Op); };
  inline GRP_ID GetInput(int i) const { return (Inputs[i]); };
  inline void SetInput(int i, GRP_ID grpId) { Inputs[i] = grpId; };
  inline GRP_ID GetGrpID() { return (GrpID); };
  inline int GetArity() { return (Op->GetArity()); };

  inline M_EXPR *GetNextHash() { return HashPtr; };
  inline void SetNextHash(M_EXPR *mexpr) { HashPtr = mexpr; };

  inline void SetNextMExpr(M_EXPR *MExpr) { NextMExpr = MExpr; };
  inline M_EXPR *GetNextMExpr() { return NextMExpr; };

  // We just fired this rule, so update dont_fire bit vector
  inline void fire_rule(int rule_no) { bit_on(RuleMask, rule_no); };

#ifdef UNIQ
  // Can I fire this rule?
  inline bool can_fire(int rule_no) { return (is_bit_off(RuleMask, rule_no)); };
#endif

  inline void set_rule_mask(BIT_VECTOR v) { RuleMask = v; };

  ub4 hash() {
    ub4 hashval = Op->hash();

    // to check the equality of the inputs
    for (int input_no = Op->GetArity(); --input_no >= 0;) hashval = lookup2(GetInput(input_no), hashval);

    return (hashval % (HtblSize - 1));
  };

  string Dump() {
    string os;

    os = (*Op).Dump();

    int Size = GetArity();
    for (int i = 0; i < Size; i++) os += " , " + to_string(Inputs[i]);

    return os;
  };

};  // class M_EXPR

/*

============================================================
class GROUP
============================================================
The main problem, and its subproblems, consist of
a search to find the cheapest MultiExpression in a GROUP,
satisfying some context.

A GROUP includes a collection of logically equivalent M_EXPR's.
The GROUP also contains logical properties shared by all M_EXPR's
in the group.

A GROUP also contains a winner's circle consisting of winners from
previous searches of the GROUP.  Note that each search may of the
GROUP may have different contexts and thus different winners.

A GROUP can also be thought of as a temporary collection, or
subquery, in a computation of the main query.

We assume the following three conditions are equivalent:
(1) There is a winner (maybe with null *plan) for some property, with done==true
(2) The group contains all possible logical and physical expressions except for
     enforcers
(3) The bit "optimized" is turned on.

And any of these imply:
(4) For any property P, the cheapest plan for property P is either in G or
 is an enforcer for P

The truth of the above assumptions depend on whether we fire all
applicable rules when the property is ANY. */

//##ModelId=3B0C0866009C
struct BIT_STATE {
  //##ModelId=3B0C086600B1
  unsigned changed : 1;  // has the group got changed or just created?
  // Used for tracing
  //##ModelId=3B0C086600C5
  unsigned exploring : 1;  // is the group being explored?
  //##ModelId=3B0C086600D9
  unsigned explored : 1;  // Has the group been explored?
  //##ModelId=3B0C086600F7
  unsigned optimizing : 1;  // is the group being optimized?
  //##ModelId=3B0C0866010B
  unsigned optimized : 1;  // has the group been optimized (completed) ?
  //##ModelId=3B0C08660129
  unsigned others : 1;
};

class GROUP {
 public:
  GROUP(M_EXPR *MExpr);  // Create a new Group containing just this MExpression
  ~GROUP();

  string Dump();
  void FastDump();

  // Find first and last (in some sense) MExpression in this Group
  inline M_EXPR *GetFirstLogMExpr() { return FirstLogMExpr; };
  inline M_EXPR *GetFirstPhysMExpr() { return FirstPhysMExpr; };
  inline void SetLastLogMExpr(M_EXPR *last) { LastLogMExpr = last; };
  inline void SetLastPhysMExpr(M_EXPR *last) { LastPhysMExpr = last; };

  // Manipulate states
  inline void init_state() { State.changed = State.explored = State.exploring = State.optimized = false; }
  inline bool is_explored() { return (State.explored); }
  inline void set_explored(bool is_explored) { State.explored = is_explored; }
  inline bool is_changed() { return (State.changed); }
  inline void set_changed(bool is_changed) { State.changed = is_changed; }
  inline bool is_optimized() { return (State.optimized); }
  void set_optimized(bool is_optimized);
  inline bool is_exploring() { return (State.exploring); }
  inline void set_exploring(bool is_exploring) { State.exploring = is_exploring; }

  // Get's
  inline LOG_PROP *get_log_prop() { return LogProp; };
  inline Cost *GetLowerBd() { return LowerBd; };
  inline double GetEstiGrpSize() { return EstiGrpSize; };
  inline int GetCount() { return count; };
  inline GRP_ID GetGroupID() { return (GroupID); };

  // Add a new MExpr to the group
  void NewMExpr(M_EXPR *MExpr);

  /*search_circle returns the state of the winner's circle for this
  context and group - it does no rule firing.  Thus it is cheap to execute.
    search_circle returns in four possible states:
  (1) There is no possibility of satisfying C
  (2) There is a non-null winner which satisfies C
  (3) More search is needed, and there has been no search
      for this property before
  (4) More search is needed, and there has been a search for
      this property before.
  See the pseudocode for search_circle (in *.cpp) for how these states arise.

          Here is how the four states are returned
          Moresearch      Return value
  (1)     F               F
  (2)     F               T
  (3)     T               F
  (4)     T               T

  search_circle could adjust the current context, but won't because:
  It is not necessary for correctness
  It is likely not very usefull

  */
  //##ModelId=3B0C0867008B
  bool search_circle(CONT *C, bool &moresearch);

#ifdef IRPROP
  // return whether the group is completely optimized or not.
  // GrpNo and moreSearch are irrelevant
  //##ModelId=3B0C08670095
  bool search_circle(int GrpNo, PHYS_PROP *, bool &moreSearch);
#endif

  // Manipulate Winner's circle
  // Return winner for this property, null if there is none
  //##ModelId=3B0C086700A7
  WINNER *GetWinner(PHYS_PROP *PhysProp);
  // If there is a winner for ReqdProp, error.
  // Create a new winner for the property ReqdProp, with these parameters.
  // Used when beginning the first search for ReqdProp.
  //##ModelId=3B0C086700B1
  void NewWinner(PHYS_PROP *ReqdProp, M_EXPR *MExpr, Cost *TotalCost, bool done);

  //##ModelId=3B0C086700BC
  void ShrinkSubGroup();

  //##ModelId=3B0C086700BD
  void DeletePhysMExpr(M_EXPR *PhysMExpr);  // delete a physical mexpr from a group

  //##ModelId=3B0C086700C6
  bool CheckWinnerDone();  // check if there is at least one winner done in this group

#ifdef FIRSTPLAN
  //##ModelId=3B0C086700CF
  void setfirstplan(bool boolean) { firstplan = boolean; };
  //##ModelId=3B0C086700D9
  bool getfirstplan() { return firstplan; };
#endif

 private:
  //##ModelId=3B0C086700E4
  GRP_ID GroupID;  // ID of this group

  //##ModelId=3B0C086700F8
  M_EXPR *FirstLogMExpr;  // first log M_EXPR in  the GROUP
  //##ModelId=3B0C08670116
  M_EXPR *LastLogMExpr;  // last log M_EXPR in  the GROUP
  //##ModelId=3B0C0867012B
  M_EXPR *FirstPhysMExpr;  // first phys M_EXPR in  the GROUP
  //##ModelId=3B0C08670149
  M_EXPR *LastPhysMExpr;  // last phys M_EXPR in  the GROUP

  //##ModelId=3B0C08670167
  struct BIT_STATE State;  //  the state of the group

  //##ModelId=3B0C08670185
  LOG_PROP *LogProp;  // Logical properties of this GROUP
  //##ModelId=3B0C086701A3
  Cost *LowerBd;  // lower bound of cost of fetching cucard tuples from disc

  // Winner's circle
  //##ModelId=3B0C086701B7
  vector<WINNER *> Winners;

  // if operator is EQJOIN, estimate the group size, else estimate group size =0
  // used for halt option
  //##ModelId=3B0C086701CA
  double EstiGrpSize;

  //##ModelId=3B0C086701E8
  int count;

  //##ModelId=3B0C086701FC
  int EstimateNumTables(M_EXPR *MExpr);

#ifdef FIRSTPLAN
  //##ModelId=3B0C0867021A
  static bool firstplan;
#endif

};  // class GROUP

/*
============================================================
WINNER
============================================================
The key idea of dynamic programming/memoization is to save the results
of searches for future use.  A WINNER is such a result.  In general a WINNER
contains the M_EXPR which won a search plus the context (CONT) used in the search.
Done = False, in a winner, means this winner is under construction; its search is
not complete.

Each group has a set of winners derived from previous searches of that group.
This set of winners is called a memo in the classic literature; here we call
it a winner's circle (cf. the GROUP class).

A winner can represent these cases, if Done is true:
(1) If MPlan is not null:
        *MPlan is the cheapest possible plan in this group with PhysProp.
        *MPlan has cost *Cost.  This derives from a successful search.
(2) If MPlan is null, and Cost is not null:
        All possible plans in this group with PhysProp cost more than *Cost.
        This derives from a search which fails because of cost.
(3) If MPlan is null, and Cost is null:
        There can be no plan in this group with PhysProp
        (Should never happen if we have enforcers)
        (also should not happen because winners are initialized with context's costs)

While the physical mexpressions of a group are being costed (i.e. Done=false),
the cheapest plan yet found, and its cost, are stored in a winner.
*/

//##ModelId=3B0C08670350
class WINNER {
 private:
  //##ModelId=3B0C08670365
  M_EXPR *MPlan;
  //##ModelId=3B0C08670379
  PHYS_PROP *PhysProp;  // PhysProp and Cost typically represent the context of
  //##ModelId=3B0C08670397
  Cost *cost;  // the most recent search which generated this winner.

  //##ModelId=3B0C086703AA
  bool Done;  // Is this a real winner; is the current search complete?
 public:
  //##ModelId=3B0C086703BE
  WINNER(M_EXPR *, PHYS_PROP *, Cost *, bool done = false);
  //##ModelId=3B0C086703DD
  ~WINNER() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_WINNER].Delete();
    delete MPlan;
    delete cost;
  };

  //##ModelId=3B0C086703DE
  inline M_EXPR *GetMPlan() { return (MPlan); };
  //##ModelId=3B0C086703E7
  inline PHYS_PROP *GetPhysProp() { return (PhysProp); };
  //##ModelId=3B0C086703E8
  inline Cost *GetCost() { return (cost); };
  //##ModelId=3B0C08680009
  inline bool GetDone() { return (Done); };
  //##ModelId=3B0C08680013
  inline void SetDone(bool value) { Done = value; };

};  // class WINNER

/*
    ============================================================
    MULTIWINNERS of a search - used only when IRPROP is defined
    ============================================================
        A M_WINNER (multiwinner) data structure will, when the group is optimized, contain a
        winner (best plan) for each interesting and relevant (i.e., in the schema) property of
        the group.  The M_WINNER elements are initialized with an infinite bound to indicate
        that no qualifying plan has yet been found.

    The idea is then to optimize a group not only for the motivating context but also for
        all the contexts stored in a M_WINNER for that group. In this way, we do not have to
        revisit that group, thus saving CPU time and memory.
*/

//##ModelId=3B0C0868017B
class M_WINNER {
 public:
  //##ModelId=3B0C08680192
  static vector<M_WINNER *> mc;
  //##ModelId=3B0C086801A4
  static Cost InfCost;

 private:
  //##ModelId=3B0C086801B7
  int wide;  // size of each element
  //##ModelId=3B0C086801D6
  M_EXPR **BPlan;
  //##ModelId=3B0C086801EA
  PHYS_PROP **PhysProp;
  //##ModelId=3B0C08680208
  Cost **Bound;

 public:
  //##ModelId=3B0C0868021B
  M_WINNER(int);
  //##ModelId=3B0C08680226
  ~M_WINNER() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_M_WINNER].Delete();
    delete[] BPlan;
    for (int i = 0; i < wide; i++) {
      delete PhysProp[i];
      if (Bound[i] != NULL) delete Bound[i];
    }
    delete[] PhysProp;
    delete[] Bound;
  };

  //##ModelId=3B0C0868022F
  inline int GetWide() { return wide; };

  // Return the requested MEXPR indexed by an integer
  inline M_EXPR *GetBPlan(int i) { return (BPlan[i]); };

  // Return the MEXPR indexed by the physical property
  inline M_EXPR *GetBPlan(PHYS_PROP *PhysProp) {
    for (int i = 0; i < wide; i++) {
      if (*GetPhysProp(i) == *PhysProp) {
        return (BPlan[i]);
      }
    }
    return (NULL);
  };

  inline void SetPhysProp(int i, PHYS_PROP *Prop) { PhysProp[i] = Prop; }

  // Return the requested physical property from multiwinner
  inline PHYS_PROP *GetPhysProp(int i) { return (PhysProp[i]); };

  // Return the upper bound of the required physical property
  inline Cost *GetUpperBd(PHYS_PROP *PhysProp) {
    for (int i = 0; i < wide; i++) {
      if (*GetPhysProp(i) == *PhysProp) {
        return (Bound[i]);
      }
    }
    return (&InfCost);
  };

  //  Update bounds, when we get new bound for the context.
  inline void SetUpperBound(Cost *NewUB, PHYS_PROP *Prop) {
    for (int i = 0; i < wide; i++) {
      if (*GetPhysProp(i) == *Prop) {
        if (Bound[i] != NULL) delete Bound[i];
        Bound[i] = NewUB;
      }
    }
  };

  // Update the MEXPR
  inline void SetBPlan(M_EXPR *Winner, int i) { BPlan[i] = Winner; };

  // print the multiwinners
  string Dump() {
    string os;
    for (int i = 0; i < wide; i++) {
      os += PhysProp[i]->Dump();
      os += ",  ";
      os += Bound[i]->Dump();
      os += ",  ";
      if (BPlan[i] != NULL)
        os += BPlan[i]->Dump();
      else
        os += "NULL";
      os += "\n";
    }
    return os;
  }
};  // class M_WINNER
