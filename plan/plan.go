package plan

import "simpleDb/ast"

func MakeLogicPlan(ast *ast.SelectStm) LogicPlan {
	scanLogicPlan := makeScanLogicPlan(ast.TableReferences)
	selectLogicPlan := makeSelectLogicPlan(scanLogicPlan, ast.Where)
}

func makeScanLogicPlan(tableRefs []ast.TableReferenceStm) ScanLogicPlan {
	// Todo.
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

func makeGroupByLogicPlan() GroupByLogicPlan {

}

func makeHavingLogicPlan() HavingLogicPlan {

}

func makeOrderByLogicPlan() OrderByLogicPlan {

}

func MakePhysicalPlan(logicPlan LogicPlan) PhysicalPlan {

}
