package plan

import "simpleDb/ast"

func MakeLogicPlan(ast *ast.SelectStm) LogicPlan {
	scanLogicPlans := makeScanLogicPlans(ast.TableReferences)
	joinLogicPlan := scanLogicPlans[0]
	if len(scanLogicPlans) >= 2 {
		joinLogicPlan = makeJoinLogicPlan(scanLogicPlans, ast.TableReferences)
	}
	selectLogicPlan := makeSelectLogicPlan(joinLogicPlan, ast.Where)
	groupByLogicPlan := makeGroupByLogicPlan(selectLogicPlan, ast.Groupby)
	havingLogicPlan := makeHavingLogicPlan(groupByLogicPlan, ast.Having)

	OrderByLogicPlan := makeOrderByLogicPlan(havingLogicPlan, ast.OrderBy)
}

func makeScanLogicPlans(tableRefs []ast.TableReferenceStm) (ret []LogicPlan) {
	for _, tableRef := range tableRefs {
		switch tableRef.Tp {
		case ast.TableReferenceTableFactorTp:
			ret = append(ret, makeScanLogicPlan(tableRef.TableReference))
		case ast.TableReferenceJoinedTableTp:
			ret = append(ret, makeScanLogicPlanFromJoin(tableRef.TableReference.())...)
		default:
			panic("unsupported table ref type")
		}
	}
	return
}

func makeScanLogicPlan(tableRefTableStm ast.TableReferencePureTableRefStm) LogicPlan {
	return NewScanLogicPlan(tableRefTableStm.TableName, tableRefTableStm.Alias)
}

func makeScanLogicPlanFromJoin() []LogicPlan {

}

// len(tableRefs) >= 2
func makeJoinLogicPlan(input []LogicPlan, tableRefs []ast.TableReferenceStm) LogicPlan {
	for i := 1; i < len(tableRefs); i++ {

	}
}

func makeSelectLogicPlan(input LogicPlan, whereStm ast.WhereStm) SelectionLogicPlan {
	return SelectionLogicPlan{
		Input: input,
		Expr:  whereStmToLogicExpr(whereStm),
	}
}

func whereStmToLogicExpr(whereStm ast.WhereStm) LogicExpr {
	// Todo
}

func makeGroupByLogicPlan(input LogicPlan, groupBy *ast.GroupByStm) GroupByLogicPlan {
	// Todo
}

func makeHavingLogicPlan(input LogicPlan, having ast.HavingStm) HavingLogicPlan {
	// Todo
}

func makeOrderByLogicPlan(input LogicPlan, orderBy *ast.OrderByStm) OrderByLogicPlan {

}

func MakePhysicalPlan(logicPlan LogicPlan) PhysicalPlan {

}
