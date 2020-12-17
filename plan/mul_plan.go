package plan

import (
	"errors"
	"simpleDb/parser"
	"simpleDb/storage"
)

type Insert struct {
	Schema string
	Table  string
	Cols   []string
	Exprs  []LogicExpr
	Values [][]byte
}

func MakeInsertPlan(table, schema string, cols []string, exprs []*parser.ExpressionStm) Insert {
	logicExprs := ExprStmsToLogicExprs(exprs, nil)
	return Insert{
		Schema: schema,
		Table:  table,
		Cols:   cols,
		Exprs:  logicExprs,
		Values: make([][]byte, len(logicExprs)),
	}
}

func (insert Insert) Execute() error {
	// Now we save the values to the table.
	// Todo
}

func (insert Insert) TypeCheck() error {
	if !storage.GetStorage().HasSchema(insert.Schema) {
		return errors.New("schema doesn't exist")
	}
	dbInfo := storage.GetStorage().GetDbInfo(insert.Schema)
	if !dbInfo.HasTable(insert.Table) {
		return errors.New("table doesn't exist")
	}
	// Todo: if a column is auto increment, do we need to skip this checking.
	if len(insert.Cols) != 0 && len(insert.Cols) != len(insert.Exprs) {
		return errors.New("columns doesn't match")
	}
	for _, expr := range insert.Exprs {
		err := expr.TypeCheck()
		if err != nil {
			return err
		}
	}
	// Now we check whether the column type match expr type, and compute expr value.
	tableInfo := dbInfo.GetTable(insert.Table)
	for i, col := range insert.Cols {
		colInfo := tableInfo.GetColumnInfo(col)
		err := colInfo.CanOp(insert.Exprs[i].toField(), storage.EqualOpType)
		if err != nil {
			return err
		}
		v, err := insert.Exprs[i].Compute()
		if err != nil {
			return err
		}
		insert.Values[i] = v
	}
	return nil
}

type Update struct{}

func MakeUpdatePlan() Update {}

func (update Update) Execute() {}

type MultiUpdate struct{}

func MakeMultiUpdatePlan() MultiUpdate {}
func (update MultiUpdate) Execute()    {}

type Delete struct{}

func MakeDeletePlan() Delete {}

func (delete Delete) Execute() {}

type Alter struct{}

func (alter Alter) Execute() {}
