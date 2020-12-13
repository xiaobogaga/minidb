package executors

import (
	"simpleDb/ast"
	"simpleDb/plan"
	"simpleDb/storage"
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

func ExecuteCreateDatabaseStm(stm *ast.CreateDatabaseStm) error {
	if stm.IfNotExist && storage.GetStorage().HasSchema(stm.DatabaseName) {
		return nil
	}
	// Create database otherwise
	storage.GetStorage().CreateSchema(stm.DatabaseName, stm.Charset, stm.Collate)
	return nil
}

func ExecuteRemoveDatabaseStm(stm *ast.DropDatabaseStm) error {
	if !storage.GetStorage().HasSchema(stm.DatabaseName) {
		return nil
	}
	storage.GetStorage().RemoveSchema(stm.DatabaseName)
	return nil
}

func ExecuteCreateTableStm(stm *ast.CreateTableStm) error {

}

func ExecuteDropTableStm(stm *ast.DropTableStm) error {}

func ExecuteInsertStm(stm *ast.InsertIntoStm) error {}

func ExecuteUpdateStm(stm *ast.UpdateStm) error {}
