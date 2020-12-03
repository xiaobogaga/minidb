package executors

import (
	"simpleDb/ast"
	"simpleDb/plan"
)

func Exec(stm ast.Stm, currentDB string) {

}

func ExecuteSelectStm(stm *ast.SelectStm, currentDB string) error {
	// we need to generate a logic plan for this selectStm.
	logicPlan, err := plan.MakeLogicPlan(stm, currentDB)
	if err != nil {
		return err
	}
	// physicalPlan := plan.MakePhysicalPlan(logicPlan)
	for {
		data := logicPlan.Execute()
		if data == nil {
			// means we have all data
			return nil
		}
		// Todo: send data to client.
	}
	return nil
}
