package executors

import (
	"simpleDb/ast"
	"simpleDb/plan"
)

func Exec(stm ast.Stm) {

}

func ExecuteSelectStm(stm *ast.SelectStm) error {
	// we need to generate a logic plan for this selectStm.
	logicPlan := plan.MakeLogicPlan(stm)
	physicalPlan := plan.MakePhysicalPlan(logicPlan)
	for {
		data := physicalPlan.Execute()
		if data == nil {
			// means we have all data
			return nil
		}
		// Todo: send data to client.
	}
	return nil
}
