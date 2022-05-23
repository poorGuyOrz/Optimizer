#include "../header/mexpression.h"

#include "../header/ssp.h"

MExression::MExression(Expression *Expr, int grpid)
    : Op(Expr->GetOp()->Clone()),
      NextMExpr(nullptr),
      GrpID((grpid == NEW_GRPID) ? Ssp->GetNewGrpID() : grpid),
      HashPtr(nullptr),
      RuleMask(0) {
  int groupID;
  Expression *input;

  // copy in the sub-expression
  int arity = GetArity();
  if (arity) {
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
      children_.push_back(groupID);
    }
  }  // if(arity)
};

MExression::MExression(MExression &other)
    : GrpID(other.GrpID),
      HashPtr(other.HashPtr),
      NextMExpr(other.NextMExpr),
      children_(other.children_),
      Op(other.Op->Clone()),
      RuleMask(other.RuleMask){};