package ast

import "simpleDb/plan"

func (stm *SelectStm) Execute() error {
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
