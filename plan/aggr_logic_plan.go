package plan

import (
	"fmt"
	"simpleDb/ast"
	"simpleDb/storage"
)

// For groupBy exprs.
type GroupByLogicPlan struct {
	Input       LogicPlan
	GroupByExpr []LogicExpr
	AggrExprs   []LogicExpr
	data        *storage.RecordBatch
	index       int
}

func (groupBy GroupByLogicPlan) Schema() storage.Schema {
	retTable := storage.SingleTableSchema{}
	ret := storage.Schema{
		Name:   "groupBy",
		Tables: []storage.SingleTableSchema{retTable},
	}
	for _, aggrExpr := range groupBy.AggrExprs {
		f := aggrExpr.toField(groupBy.Input)
		retTable.Columns = append(retTable.Columns, f)
	}
	return ret
}

func (groupBy GroupByLogicPlan) String() string {
	return fmt.Sprintf("GroupByLogicPlan: %s groupBy %s", groupBy.Input, groupBy.GroupByExpr)
}

func (groupBy GroupByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{groupBy.Input}
}

func (groupBy GroupByLogicPlan) TypeCheck() error {
	err := groupBy.Input.TypeCheck()
	if err != nil {
		return err
	}
	for _, expr := range groupBy.GroupByExpr {
		err = expr.TypeCheck(groupBy.Input)
		if err != nil {
			return err
		}
	}
	// Now we check whether aggrExprs is okay.
	for _, aggrExpr := range groupBy.AggrExprs {
		err = aggrExpr.AggrTypeCheck(groupBy.GroupByExpr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (groupBy GroupByLogicPlan) Execute() *storage.RecordBatch {
	if groupBy.data == nil {
		groupBy.InitializeData()
	}

}

func (groupBy GroupByLogicPlan) InitializeData() {
	if groupBy.data != nil {
		return
	}

}

func (groupBy GroupByLogicPlan) Reset() {}

// For Having condition
type HavingLogicPlan struct {
	Input GroupByLogicPlan
	Expr  LogicExpr
}

func (having HavingLogicPlan) Schema() storage.Schema {
	// Should be the same schema as Input.
	return having.Input.Schema()
}

func (having HavingLogicPlan) String() string {
	return fmt.Sprintf("HavingLogicPlan: %s having %s", having.Input, having.Expr)
}

func (having HavingLogicPlan) Child() []LogicPlan {
	return []LogicPlan{having.Input}
}

func (having HavingLogicPlan) TypeCheck() error {
	err := having.Input.TypeCheck()
	if err != nil {
		return err
	}
	return having.Expr.AggrTypeCheck(having.Input.GroupByExpr)
}

func (having HavingLogicPlan) Execute() *storage.RecordBatch {}

func (having HavingLogicPlan) Reset() {
	having.Input.Reset()
}

func MakeAggreLogicPlan(input LogicPlan, ast *ast.SelectStm) (LogicPlan, error) {
	groupByLogicPlan := makeGroupByLogicPlan(input, ast.Groupby)
	// Having similar to projections for aggregation, the expr must be either included in the group by expr.
	// or must be an aggregation function.
	havingLogicPlan := makeHavingLogicPlan(groupByLogicPlan, ast.Having)
	// Order by similar to projections for aggregation, the expr must be either included in the group by expr,
	// or must be an aggregation function.
	orderByLogicPlan := makeOrderByLogicPlan(havingLogicPlan, ast.OrderBy, true)
	limitLogicPlan := makeLimitLogicPlan(orderByLogicPlan, ast.LimitStm)
	return limitLogicPlan, limitLogicPlan.TypeCheck()
}

func makeHavingLogicPlan(input GroupByLogicPlan, having ast.HavingStm) HavingLogicPlan {
	return HavingLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(having),
	}
}

func makeGroupByLogicPlan(input LogicPlan, groupBy *ast.GroupByStm) GroupByLogicPlan {
	return GroupByLogicPlan{
		Input:       input,
		GroupByExpr: ExprStmsToLogicExprs(*groupBy),
	}
}

func ExprStmsToLogicExprs(expressions []*ast.ExpressionStm) (ret []LogicExpr) {
	for _, expr := range expressions {
		ret = append(ret, ExprStmToLogicExpr(expr))
	}
	return ret
}
