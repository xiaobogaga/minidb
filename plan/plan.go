package plan

import "simpleDb/ast"

func MakeLogicPlan(ast *ast.SelectStm) LogicPlan {
	scanLogicPlans := makeScanLogicPlans(ast.TableReferences)
	joinLogicPlan := scanLogicPlans[0]
	if len(scanLogicPlans) >= 2 {
		joinLogicPlan = makeJoinLogicPlan(scanLogicPlans)
	}
	selectLogicPlan := makeSelectLogicPlan(joinLogicPlan, ast.Where)
	groupByLogicPlan := makeGroupByLogicPlan(selectLogicPlan, ast.Groupby)
	havingLogicPlan := makeHavingLogicPlan(groupByLogicPlan, ast.Having)
	projectionsLogicPlan := makeProjectionLogicPlan(havingLogicPlan, ast.SelectExpressions)
	orderByLogicPlan := makeOrderByLogicPlan(projectionsLogicPlan, ast.OrderBy)
	limitLogicPlan := makeLimitLogicPlan(orderByLogicPlan, ast.LimitStm)
	return limitLogicPlan
}

func makeScanLogicPlans(tableRefs []ast.TableReferenceStm) (ret []LogicPlan) {
	for _, tableRef := range tableRefs {
		switch tableRef.Tp {
		case ast.TableReferenceTableFactorTp:
			ret = append(ret, makeScanLogicPlan(tableRef.TableReference.(ast.TableReferenceTableFactorStm)))
		case ast.TableReferenceJoinTableTp: // Build scanLogicPlan for the join op.
			ret = append(ret, makeScanLogicPlanForJoin(tableRef.TableReference.(ast.JoinedTableStm)))
		default:
			panic("unsupported table ref type")
		}
	}
	return
}

func makeScanLogicPlan(tableRefTableFactorStm ast.TableReferenceTableFactorStm) LogicPlan {
	switch tableRefTableFactorStm.Tp {
	case ast.TableReferencePureTableNameTp:
		// Todo
		return ScanLogicPlan{}
	case ast.TableReferenceTableSubQueryTp, ast.TableReferenceSubTableReferenceStmTP:
		panic("doesn't support sub query currently")
	}
	return nil
}

func makeScanLogicPlanForJoin(joinTableStm ast.JoinedTableStm) JoinLogicPlan {
	// a inorder traversal to build logic plan.
	leftLogicPlan := makeScanLogicPlan(joinTableStm.TableReference)
	rightLogicPlan := buildLogicPlanForTableReferenceStm(joinTableStm.JoinedTableReference)
	return JoinLogicPlan{
		LeftLogicPlan:  leftLogicPlan,
		RightLogicPlan: rightLogicPlan,
		JoinType:       joinTableStm.JoinTp,
	}
}

func buildLogicPlanForTableReferenceStm(tableRef ast.TableReferenceStm) LogicPlan {
	switch tableRef.Tp {
	case ast.TableReferenceTableFactorTp:
		return makeScanLogicPlan(tableRef.TableReference.(ast.TableReferenceTableFactorStm))
	case ast.TableReferenceJoinTableTp:
		return makeScanLogicPlanForJoin(tableRef.TableReference.(ast.JoinedTableStm))
	}
	return nil
}

// len(tableRefs) >= 2
func makeJoinLogicPlan(input []LogicPlan) LogicPlan {
	leftLogicPlan := input[0]
	for i := 1; i < len(input); i++ {
		rightLogicPlan := input[i]
		leftLogicPlan = JoinLogicPlan{
			LeftLogicPlan:  leftLogicPlan,
			RightLogicPlan: rightLogicPlan,
			JoinType:       ast.InnerJoin,
		}
	}
	return leftLogicPlan
}

func makeSelectLogicPlan(input LogicPlan, whereStm ast.WhereStm) SelectionLogicPlan {
	return SelectionLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(whereStm),
	}
}

func ExprStmToLogicExpr(expr *ast.ExpressionStm) LogicExpr {
	var leftLogicExpr, rightLogicExpr LogicExpr
	_, isLeftExprExprStm := expr.LeftExpr.(*ast.ExpressionStm)
	if isLeftExprExprStm {
		leftLogicExpr = ExprStmToLogicExpr(expr.LeftExpr.(*ast.ExpressionStm))
	} else {
		leftLogicExpr = ExprTermStmToLogicExpr(expr.LeftExpr.(*ast.ExpressionTerm))
	}
	if expr.RightExpr == nil {
		return leftLogicExpr
	}
	_, isRightExprExprStm := expr.RightExpr.(*ast.ExpressionStm)
	if isRightExprExprStm {
		rightLogicExpr = ExprStmToLogicExpr(expr.RightExpr.(*ast.ExpressionStm))
	} else {
		rightLogicExpr = ExprTermStmToLogicExpr(expr.RightExpr.(*ast.ExpressionTerm))
	}
	return buildLogicExprWithOp(leftLogicExpr, rightLogicExpr, expr.Op)
}

func buildLogicExprWithOp(leftLogicExpr, rightLogicExpr LogicExpr, op ast.ExpressionOp) LogicExpr {
	// Todo

}

func ExprTermStmToLogicExpr(exprTerm *ast.ExpressionTerm) LogicExpr {
	if exprTerm.UnaryOp == ast.NoneUnaryOpTp {
		switch exprTerm.Tp {
		case ast.LiteralExpressionTermTP:
			return LiteralExprToLiteralLogicPlan(exprTerm.RealExprTerm.(ast.LiteralExpressionStm))
		case ast.IdentifierExpressionTermTP:
			return IdentifierExprToIdentifierLogicPlan()
		}
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

func OrderedExpressionToOrderedExprs(orderedExprs []*ast.OrderedExpressionStm) OrderedExpr {
	// Todo
}

func makeHavingLogicPlan(input LogicPlan, having ast.HavingStm) HavingLogicPlan {
	// Todo
	return HavingLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(having),
	}
}

func makeOrderByLogicPlan(input LogicPlan, orderBy *ast.OrderByStm) OrderByLogicPlan {
	return OrderByLogicPlan{
		Input: input,
		Expr:  OrderedExpressionToOrderedExprs(orderBy.Expressions),
	}
}

func makeLimitLogicPlan(input LogicPlan, limitStm *ast.LimitStm) LimitLogicPlan {
	return LimitLogicPlan{
		Input:  input,
		Count:  limitStm.Count,
		Offset: limitStm.Offset,
	}
}

func makeProjectionLogicPlan(input LogicPlan, selectExprStm *ast.SelectExpressionStm) ProjectionLogicPlan {
	projectionLogicPlan := ProjectionLogicPlan{
		Input: input,
	}
	// Todo
	return projectionLogicPlan
}

func MakePhysicalPlan(logicPlan LogicPlan) PhysicalPlan {

}
