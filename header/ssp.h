// SearchSpace.H - Search Space

#pragma once
#include "../header/query.h"

#define NEW_GRPID -1  // used by SearchSpace::CopyIn and MExression::MExression
// means need to create a new group
#define NEW_GRPID_NOWIN -2  // use by SearchSpace::CopyIn.  Means NEW_GRPID, and this is a
// subgroup of DUMMY so don't init nontrivial winners
class SearchSpace;
class Group;
class MExression;
class WINNER;
class M_WINNER;

/*
============================================================
SEARCH SPACE - class SearchSpace
============================================================
We borrow the term Search Space from AI, where it is a tool for solving a
problem  In query optimization the problem is to find the cheapest plan
for a given query subject to certain context (class CONT).

A Search Space typically consists of a collection of possible solutions
to the problem and its subproblems. Dynamic Programming and
Memoization are two approaches to using a Search Space to solve a problem.
Both Dynamic Programming and Memoization partition the possible solutions
by logical equivalence. We will call each partition a Group.  Thus a Group
contains a collection of logically equivalent expressions.

In our setting of query optimization, logical equivalence is query equivalence.
Each Group corresponds to a temporary collection, or a subquery, in a
computation of the main query.  If the main query is A join B join C, then
there are GROUPs representing A join B, A join C, C, etc.

In our approach to query optimization, each possible solution in the Search
Space is represented compactly, by what we call a Multi-expression
(class MExression).  A solution in the search space can also be represented in
more detail by an expression (class Expression).
*/
// Search Space
class SearchSpace {
 public:
  MExression **HashTbl;  // To identify duplicate MExprs

  SearchSpace();

  void Init();  // Create some default number of empty Groups, the number
                // depending on the initial query.  Read in initial query.

  ~SearchSpace();

  void optimize();  // Later add a conditon.  Prepare the SearchSpace so an optimal plan can be found

  // Convert the Expression into a Mexpr.
  // If Mexpr is not already in the search space, then copy Mexpr into the
  // search space and return the new Mexpr.
  // If Mexpr is already in the search space, then either throw it away or
  // merge groups.
  // GrpID is the ID of the group where Mexpr will be put.  If GrpID is
  // NEW_GRPID(-1), make a new group with that ID and return its value in GrpID.
  MExression *CopyIn(Expression *Expr, int &GrpID);

  // Copy out the final plan.  Recursive, each time increasing tabs by one, so the plan is indented.
  void CopyOut(int GrpID, PHYS_PROP *PhysProp, int tabs);

  // return the next available grpID in SearchSpace
  inline int GetNewGrpID() { return (++NewGrpID); };

  // return the ID of the Root group
  inline int GetRootGID() { return (RootGID); };

  // return the specific group
  inline Group *GetGroup(int Gid) { return Groups[Gid]; };

  // If another expression in the search space is identical to MExpr, return
  //  it, else return nullptr.
  //  Identical means operators and arguments, and input groups are the same.
  MExression *FindDup(MExression &MExpr);

  // When a duplicate is found in two groups they should be merged into
  //  the same group.  We always merge bigger group_no group to smaller one.
  int MergeGroups(int group_no1, int group_no2);
  // int MergeGroups(Group & ToGroup, Group & FromGroup);

  void ShrinkGroup(int group_no);  // shrink the group marked completed
  void Shrink();                   // shrink the ssp

  bool IsChanged();  // is the ssp changed?

  string Dump();
  void FastDump();

  string DumpChanged();
  string DumpHashTable();

 private:
  int RootGID;       // ID of the oldest, root group.  Set to NEW_GRPID in SearchSpace""Init then never changes
  int InitGroupNum;  //(seems to be the same as RootGID)
  int NewGrpID;      // ID of the newest group, initially -1

  // Collection of Groups, indexed by int
  vector<Group *> Groups;

};  // class SearchSpace

/*
   ============================================================
   MULTI_EXPRESSIONS - class M_EXP
   ============================================================
   MExression is a compact form of Expression which utilizes sharing.  Inputs are
   GROUPs instead of EXPRs, so the MExression embodies several EXPRs.
   All searching is done with M_EXPRs, so each contains lots of state.

*/
class MExression {
 private:
  MExression *HashPtr;  // list within hash bucket
  BIT_VECTOR RuleMask;  // If 1, do not fire rule with that index
  int counter;          // to keep track of how many winners point to this MEXPR
  Operator *Op;         // Operator
  int *Inputs;
  int GrpID;  // I reside in this group

  // link to the next mexpr in the same group
  MExression *NextMExpr;

 public:
  ~MExression() {
    if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR].Delete();
    if (GetArity()) {
      delete[] Inputs;
    }

    delete Op;
  };

  // Transform an Expression into an MExression.  May involve creating new Groups.
  //  GrpID is the ID of the group where the MExression will be put.  If GrpID is
  //  NEW_GRPID(-1), make a new group with that ID.  (Same as SearchSpace::CopyIn)
  MExression(Expression *Expr, int grpid)
      : Op(Expr->GetOp()->Clone()),
        NextMExpr(nullptr),
        GrpID((grpid == NEW_GRPID) ? Ssp->GetNewGrpID() : grpid),
        HashPtr(nullptr),
        RuleMask(0) {
    int groupID;
    Expression *input;
    counter = 0;

    if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR].New();

    // copy in the sub-expression
    int arity = GetArity();
    if (arity) {
      Inputs = new int[arity];
      for (int i = 0; i < arity; i++) {
        input = Expr->GetInput(i);

        if (input->GetOp()->is_leaf())  // deal with LeafOperator, sharing the existing group
          groupID = ((LeafOperator *)input->GetOp())->GetGroup();
        else {
          // create a new sub group
          if (Op->GetName() == "DUMMY")
            groupID = NEW_GRPID_NOWIN;  // DUMMY subgroups have only trivial winners
          else
            groupID = NEW_GRPID;
          MExression *MExpr = Ssp->CopyIn(input, groupID);
        }

        Inputs[i] = groupID;
      }
    }  // if(arity)
  };

  MExression(MExression &other)
      : GrpID(other.GrpID),
        HashPtr(other.HashPtr),
        NextMExpr(other.NextMExpr),
        Op(other.Op->Clone()),
        RuleMask(other.RuleMask) {
    if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR].New();

    // Inputs are the only member data left to copy.
    int arity = Op->GetArity();
    if (arity) {
      Inputs = new int[arity];
      while (--arity >= 0) Inputs[arity] = other.GetInput(arity);
    }
  };

  inline int GetCounter() { return counter; };
  inline void IncCounter() { counter++; };
  inline void DecCounter() {
    if (counter != 0) counter--;
  };
  inline Operator *GetOp() { return (Op); };
  inline int GetInput(int i) const { return (Inputs[i]); };
  inline void SetInput(int i, int grpId) { Inputs[i] = grpId; };
  inline int GetGrpID() { return (GrpID); };
  inline int GetArity() { return (Op->GetArity()); };

  inline MExression *GetNextHash() { return HashPtr; };
  inline void SetNextHash(MExression *mexpr) { HashPtr = mexpr; };

  inline void SetNextMExpr(MExression *MExpr) { NextMExpr = MExpr; };
  inline MExression *GetNextMExpr() { return NextMExpr; };

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
    for (int i = 0; i < Size; i++) os += ", input: " + to_string(Inputs[i]);

    return os;
  };

};  // class MExression

/*

============================================================
class Group
============================================================
The main problem, and its subproblems, consist of
a search to find the cheapest MultiExpression in a Group,
satisfying some context.

A Group includes a collection of logically equivalent MExression's.
The Group also contains logical properties shared by all MExression's
in the group.

A Group also contains a winner's circle consisting of winners from
previous searches of the Group.  Note that each search may of the
Group may have different contexts and thus different winners.

A Group can also be thought of as a temporary collection, or
subquery, in a computation of the main query.

We assume the following three conditions are equivalent:
(1) There is a winner (maybe with nullptr *plan) for some property, with done==true
(2) The group contains all possible logical and physical expressions except for
     enforcers
(3) The bit "optimized" is turned on.

And any of these imply:
(4) For any property P, the cheapest plan for property P is either in G or
 is an enforcer for P

The truth of the above assumptions depend on whether we fire all
applicable rules when the property is ANY. */

struct BIT_STATE {
  unsigned changed : 1;  // has the group got changed or just created?
  // Used for tracing
  unsigned exploring : 1;   // is the group being explored?
  unsigned explored : 1;    // Has the group been explored?
  unsigned optimizing : 1;  // is the group being optimized?
  unsigned optimized : 1;   // has the group been optimized (completed) ?
  unsigned others : 1;
};

class Group {
 public:
  Group(MExression *MExpr);  // Create a new Group containing just this MExpression
  ~Group();

  string Dump();
  void FastDump();

  // Find first and last (in some sense) MExpression in this Group
  inline MExression *GetFirstLogMExpr() { return FirstLogMExpr; };
  inline MExression *GetFirstPhysMExpr() { return FirstPhysMExpr; };
  inline void SetLastLogMExpr(MExression *last) { LastLogMExpr = last; };
  inline void SetLastPhysMExpr(MExression *last) { LastPhysMExpr = last; };

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
  inline int GetGroupID() { return (GroupID); };

  // Add a new MExpr to the group
  void NewMExpr(MExression *MExpr);

  /*search_circle returns the state of the winner's circle for this
  context and group - it does no rule firing.  Thus it is cheap to execute.
    search_circle returns in four possible states:
  (1) There is no possibility of satisfying C
  (2) There is a non-nullptr winner which satisfies C
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
  bool search_circle(CONT *C, bool &moresearch);

#ifdef IRPROP
  // return whether the group is completely optimized or not.
  // GrpNo and moreSearch are irrelevant
  bool search_circle(int GrpNo, PHYS_PROP *, bool &moreSearch);
#endif

  // Manipulate Winner's circle
  // Return winner for this property, nullptr if there is none
  WINNER *GetWinner(PHYS_PROP *PhysProp);
  // If there is a winner for ReqdProp, error.
  // Create a new winner for the property ReqdProp, with these parameters.
  // Used when beginning the first search for ReqdProp.
  void NewWinner(PHYS_PROP *ReqdProp, MExression *MExpr, Cost *TotalCost, bool done);

  void ShrinkSubGroup();

  void DeletePhysMExpr(MExression *PhysMExpr);  // delete a physical mexpr from a group

  bool CheckWinnerDone();  // check if there is at least one winner done in this group

#ifdef FIRSTPLAN
  void setfirstplan(bool boolean) { firstplan = boolean; };
  bool getfirstplan() { return firstplan; };
#endif

 private:
  int GroupID;  // ID of this group

  MExression *FirstLogMExpr;   // first log MExression in  the Group
  MExression *LastLogMExpr;    // last log MExression in  the Group
  MExression *FirstPhysMExpr;  // first phys MExression in  the Group
  MExression *LastPhysMExpr;   // last phys MExression in  the Group

  struct BIT_STATE State;  //  the state of the group

  LOG_PROP *LogProp;  // Logical properties of this Group
  Cost *LowerBd;      // lower bound of cost of fetching cucard tuples from disc

  // Winner's circle
  vector<WINNER *> Winners;

  // if operator is EQJOIN, estimate the group size, else estimate group size =0
  // used for halt option
  double EstiGrpSize;

  int count;

  int EstimateNumTables(MExression *MExpr);

#ifdef FIRSTPLAN
  static bool firstplan;
#endif

};  // class Group

/*
============================================================
WINNER
============================================================
The key idea of dynamic programming/memoization is to save the results
of searches for future use.  A WINNER is such a result.  In general a WINNER
contains the MExression which won a search plus the context (CONT) used in the search.
Done = False, in a winner, means this winner is under construction; its search is
not complete.

Each group has a set of winners derived from previous searches of that group.
This set of winners is called a memo in the classic literature; here we call
it a winner's circle (cf. the Group class).

A winner can represent these cases, if Done is true:
(1) If MPlan is not nullptr:
        *MPlan is the cheapest possible plan in this group with PhysProp.
        *MPlan has cost *Cost.  This derives from a successful search.
(2) If MPlan is nullptr, and Cost is not nullptr:
        All possible plans in this group with PhysProp cost more than *Cost.
        This derives from a search which fails because of cost.
(3) If MPlan is nullptr, and Cost is nullptr:
        There can be no plan in this group with PhysProp
        (Should never happen if we have enforcers)
        (also should not happen because winners are initialized with context's costs)

While the physical mexpressions of a group are being costed (i.e. Done=false),
the cheapest plan yet found, and its cost, are stored in a winner.
*/

class WINNER {
 private:
  MExression *MPlan;
  PHYS_PROP *PhysProp;  // PhysProp and Cost typically represent the context of
  Cost *cost;           // the most recent search which generated this winner.

  bool Done;  // Is this a real winner; is the current search complete?
 public:
  WINNER(MExression *, PHYS_PROP *, Cost *, bool done = false);
  ~WINNER() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_WINNER].Delete();
    delete MPlan;
    delete cost;
  };

  inline MExression *GetMPlan() { return (MPlan); };
  inline PHYS_PROP *GetPhysProp() { return (PhysProp); };
  inline Cost *GetCost() { return (cost); };
  inline bool GetDone() { return (Done); };
  inline void SetDone(bool value) { Done = value; };
};

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

class M_WINNER {
 public:
  static vector<M_WINNER *> mc;
  static Cost InfCost;

 private:
  int wide;  // size of each element
  MExression **BPlan;
  PHYS_PROP **PhysProp;
  Cost **Bound;

 public:
  M_WINNER(int);
  ~M_WINNER() {
    if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_M_WINNER].Delete();
    delete[] BPlan;
    for (int i = 0; i < wide; i++) {
      delete PhysProp[i];
      if (Bound[i] != nullptr) delete Bound[i];
    }
    delete[] PhysProp;
    delete[] Bound;
  };

  inline int GetWide() { return wide; };

  // Return the requested MEXPR indexed by an integer
  inline MExression *GetBPlan(int i) { return (BPlan[i]); };

  // Return the MEXPR indexed by the physical property
  inline MExression *GetBPlan(PHYS_PROP *PhysProp) {
    for (int i = 0; i < wide; i++) {
      if (*GetPhysProp(i) == *PhysProp) {
        return (BPlan[i]);
      }
    }
    return (nullptr);
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
        if (Bound[i] != nullptr) delete Bound[i];
        Bound[i] = NewUB;
      }
    }
  };

  // Update the MEXPR
  inline void SetBPlan(MExression *Winner, int i) { BPlan[i] = Winner; };

  // print the multiwinners
  string Dump() {
    string os;
    for (int i = 0; i < wide; i++) {
      os += PhysProp[i]->Dump();
      os += ",  ";
      os += Bound[i]->Dump();
      os += ",  ";
      if (BPlan[i] != nullptr)
        os += BPlan[i]->Dump();
      else
        os += "nullptr";
      os += "\n";
    }
    return os;
  }
};  // class M_WINNER
