package plan

import (
	"errors"
	"fmt"
	"minidb/parser"
	"minidb/storage"
	"strings"
)

type Insert struct {
	Schema string
	Table  string
	Cols   []string
	Values []LogicExpr
}

func MakeInsertPlan(stm *parser.InsertIntoStm, currentDB string) Insert {
	schemaName, tableName, _ := getSchemaTableName(stm.TableName, currentDB)
	logicExprs := ExprStmsToLogicExprs(stm.Values, nil)
	return Insert{
		Schema: schemaName,
		Table:  tableName,
		Cols:   stm.Cols,
		Values: logicExprs,
	}
}

func (insert Insert) Execute() error {
	// Now we save the values to the table.
	dbInfo := storage.GetStorage().GetDbInfo(insert.Schema)
	tableInfo := dbInfo.GetTable(insert.Table)
	// Prepare the columns we need to insert.
	var realCols []string
	if len(insert.Cols) == 0 {
		// The cols should be the table columns.
		realCols = make([]string, len(tableInfo.TableSchema.Columns)-1)
		for i, col := range tableInfo.TableSchema.Columns {
			// skip row index column.
			if i == 0 {
				continue
			}
			realCols[i-1] = col.Name
		}
	} else {
		realCols = make([]string, len(insert.Cols))
		for i, col := range insert.Cols {
			_, _, realCols[i] = getSchemaTableColumnName(col)
		}
	}
	// Now we compute the values and then insert.
	values := make([][]byte, len(insert.Values))
	for i, expr := range insert.Values {
		v, err := expr.Compute()
		if err != nil {
			return err
		}
		values[i] = v
	}
	tableInfo.InsertData(realCols, values)
	return nil
}

func (insert Insert) TypeCheckForNoCols() error {
	dbInfo := storage.GetStorage().GetDbInfo(insert.Schema)
	tableInfo := dbInfo.GetTable(insert.Table)
	// One extra row index column
	if len(insert.Values) != len(tableInfo.TableSchema.Columns)-1 {
		return errors.New("values doesn't match table columns")
	}
	// Now we check col, Expr match one by one
	for i, value := range insert.Values {
		err := value.TypeCheck()
		if err != nil {
			return err
		}
		colInfo := tableInfo.TableSchema.Columns[i+1]
		// Expr type must match to column type.
		err = colInfo.CanOp(value.toField(), storage.EqualOpType)
		if err != nil {
			return err
		}
	}
	return nil
}

func (insert Insert) HasColumn(colName string) bool {
	for _, col := range insert.Cols {
		_, _, realCol := getSchemaTableColumnName(col)
		if realCol == colName {
			return true
		}
	}
	return false
}

func (insert Insert) TypeCheck() error {
	if !storage.GetStorage().HasSchema(insert.Schema) {
		return errors.New("schema doesn't exist")
	}
	dbInfo := storage.GetStorage().GetDbInfo(insert.Schema)
	if !dbInfo.HasTable(insert.Table) {
		return errors.New("table doesn't exist")
	}
	if len(insert.Cols) != 0 && len(insert.Cols) != len(insert.Values) {
		return errors.New("insert columns doesn't match")
	}
	if len(insert.Cols) == 0 {
		// when no cols in insert. then means the data must be inserted according to the column defined order.
		return insert.TypeCheckForNoCols()
	}
	// Now cols are not empty.
	// some columns cannot be missing.
	tableInfo := dbInfo.GetTable(insert.Table)
	for _, col := range tableInfo.TableSchema.Columns {
		if col.CanIgnoreInInsert() {
			continue
		}
		if !insert.HasColumn(col.Name) {
			return errors.New(fmt.Sprintf("cannot missing column %s", col.Name))
		}
	}

	// Now we check whether the column type match Expr type.
	for i, col := range insert.Cols {
		err := insert.Values[i].TypeCheck()
		if err != nil {
			return err
		}
		_, _, realCol := getSchemaTableColumnName(col)
		colInfo := tableInfo.GetColumnInfo(realCol)
		if colInfo == nil {
			return errors.New("unknown column")
		}
		err = colInfo.CanOp(insert.Values[i].toField(), storage.EqualOpType)
		if err != nil {
			return err
		}
	}
	return nil
}

type Update struct {
	DefaultSchema string
	TableName     string
	Input         LogicPlan
	Assignments   []AssignmentExpr
}

type AssignmentExpr struct {
	Col  string
	Expr LogicExpr
}

func AssignmentStmToAssignmentExprs(assignments []*parser.AssignmentStm, input LogicPlan) []AssignmentExpr {
	ret := make([]AssignmentExpr, len(assignments))
	for i, expr := range assignments {
		ret[i] = AssignmentExpr{
			Col:  expr.ColName,
			Expr: ExprStmToLogicExpr(expr.Value, input),
		}
	}
	return ret
}

// The update works as follows:
// We start with a select statement to get the row primary key. Then according to the
// primary key, we update the value accordingly.
func MakeUpdatePlan(stm *parser.UpdateStm, currentDB string) Update {
	inputPlan, _ := makeScanLogicPlan(stm.TableRefs.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
	selectLogicPlan := makeSelectLogicPlan(inputPlan, stm.Where)
	orderByLogicPlan := makeOrderByLogicPlan(selectLogicPlan, stm.OrderBy, false)
	selectAllExpr := parser.SelectExpressionStm{
		Tp: parser.StarSelectExpressionTp,
	}
	projectionLogicPlan := makeProjectionLogicPlan(orderByLogicPlan, &selectAllExpr)
	limitLogicPlan := makeLimitLogicPlan(projectionLogicPlan, stm.Limit)
	return Update{
		DefaultSchema: currentDB,
		TableName: stm.TableRefs.TableReference.(parser.TableReferenceTableFactorStm).
			TableFactorReference.(parser.TableReferencePureTableRefStm).TableName,
		Input:       limitLogicPlan,
		Assignments: AssignmentStmToAssignmentExprs(stm.Assignments, limitLogicPlan),
	}
}

func (update Update) Execute() error {
	for {
		data := update.Input.Execute()
		if data == nil {
			return nil
		}
		updateTableData(update.DefaultSchema, update.TableName, data, update.Assignments)
	}
	return nil
}

func (update Update) TypeCheck() error {
	err := update.Input.TypeCheck()
	if err != nil {
		return err
	}
	// type check on assignments.
	schemaName, tableName, _ := getSchemaTableName(update.TableName, update.DefaultSchema)
	for _, assign := range update.Assignments {
		err := assign.Expr.TypeCheck()
		if err != nil {
			return err
		}
		tableInfo := storage.GetStorage().GetDbInfo(schemaName).GetTable(tableName)
		f := tableInfo.GetColumnInfo(assign.Col)
		if f == nil {
			return errors.New(fmt.Sprintf("cannot find such column %s", assign.Col))
		}
		err = f.CanOp(assign.Expr.toField(), storage.EqualOpType)
		if err != nil {
			return err
		}
	}
	// Now we check whether the column can matched to assignment.
	return nil
}

type MultiUpdate struct {
	DefaultSchema string
	Assignments   []AssignmentExpr
	Input         LogicPlan
}

func MakeMultiUpdatePlan(stm *parser.MultiUpdateStm, currentDB string) MultiUpdate {
	scanLogicPlans, _ := makeScanLogicPlans(stm.TableRefs, currentDB)
	joinLogicPlan := makeJoinLogicPlan(scanLogicPlans)
	selectLogicPlan := makeSelectLogicPlan(joinLogicPlan, stm.Where)
	selectAllExpr := parser.SelectExpressionStm{
		Tp: parser.StarSelectExpressionTp,
	}
	projectionLogicPlan := makeProjectionLogicPlan(selectLogicPlan, &selectAllExpr)
	return MultiUpdate{
		DefaultSchema: currentDB,
		Input:         projectionLogicPlan,
		Assignments:   AssignmentStmToAssignmentExprs(stm.Assignments, projectionLogicPlan),
	}
}

// name can be something like db.testTable.c1
func getSchemaTableColumnName(name string) (schema, table, col string) {
	ss := strings.Split(name, ".")
	switch len(ss) {
	case 3:
		schema = ss[0]
		table = ss[1]
		col = ss[2]
	case 2:
		table = ss[0]
		col = ss[1]
	case 1:
		col = ss[0]
	}
	return
}

func (update MultiUpdate) Execute() error {
	for {
		data := update.Input.Execute()
		if data == nil {
			return nil
		}
		// Todo
		updateTableData(update.DefaultSchema, "", data, update.Assignments)
	}
}

func updateTableData(schemaName, tableName string, data *storage.RecordBatch, assignments []AssignmentExpr) {
	schemaName, tableName, _ = getSchemaTableName(tableName, schemaName)
	// schema := input.Schema()
	for i := 0; i < data.RowCount(); i++ {
		for _, assign := range assignments {
			ret := assign.Expr.EvaluateRow(i, data)
			index, _ := data.RowIndex(tableName, i)
			tableInfo := storage.GetStorage().GetDbInfo(schemaName).GetTable(tableName)
			tableInfo.UpdateData(assign.Col, index, ret)
		}
	}
}

func (update MultiUpdate) TypeCheck() error {
	return update.Input.TypeCheck()
}

type Delete struct {
	DefaultSchemaName string
	TableName         string
	Input             LogicPlan
}

func MakeDeletePlan(stm *parser.SingleDeleteStm, currentDB string) Delete {
	inputPlan, _ := makeScanLogicPlan(stm.TableRef.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
	selectLogicPlan := makeSelectLogicPlan(inputPlan, stm.Where)
	orderByLogicPlan := makeOrderByLogicPlan(selectLogicPlan, stm.OrderBy, false)
	selectAllExpr := parser.SelectExpressionStm{
		Tp: parser.StarSelectExpressionTp,
	}
	projectionLogicPlan := makeProjectionLogicPlan(orderByLogicPlan, &selectAllExpr)
	limitLogicPlan := makeLimitLogicPlan(projectionLogicPlan, stm.Limit)
	return Delete{
		DefaultSchemaName: currentDB,
		Input:             limitLogicPlan,
		TableName: stm.TableRef.TableReference.(parser.TableReferenceTableFactorStm).
			TableFactorReference.(parser.TableReferencePureTableRefStm).TableName,
	}
}

func (delete Delete) Execute() error {
	for {
		data := delete.Input.Execute()
		if data == nil {
			return nil
		}
		deleteTableData(data, delete.DefaultSchemaName, delete.TableName)
	}
	return nil
}

func deleteTableData(data *storage.RecordBatch, defaultDB string, tables ...string) {
	for _, table := range tables {
		schemaName, tableName, _ := getSchemaTableName(table, defaultDB)
		for i := 0; i < data.RowCount(); i++ {
			index, _ := data.RowIndex(tableName, i)
			dbInfo := storage.GetStorage().GetDbInfo(schemaName)
			tableInfo := dbInfo.GetTable(tableName)
			// after remove one row. we need to decrease row index.
			tableInfo.DeleteRow(index - i)
		}
	}
}

func (delete Delete) TypeCheck() error {
	return delete.Input.TypeCheck()
}

type MultiDelete struct {
	Input     LogicPlan
	Tables    []string
	DefaultDB string
}

func MakeMultiDeletePlan(stm *parser.MultiDeleteStm, currentDB string) MultiDelete {
	scanLogicPlans, _ := makeScanLogicPlans(stm.TableReferences, currentDB)
	joinLogicPlan := makeJoinLogicPlan(scanLogicPlans)
	selectLogicPlan := makeSelectLogicPlan(joinLogicPlan, stm.Where)
	selectAllExpr := parser.SelectExpressionStm{
		Tp: parser.StarSelectExpressionTp,
	}
	projectionLogicPlan := makeProjectionLogicPlan(selectLogicPlan, &selectAllExpr)
	return MultiDelete{Input: projectionLogicPlan, Tables: stm.TableNames, DefaultDB: currentDB}
}

func (delete MultiDelete) Execute() error {
	for {
		data := delete.Input.Execute()
		if data == nil {
			return nil
		}
		deleteTableData(data, delete.DefaultDB, delete.Tables...)
	}
}

func (delete MultiDelete) TypeCheck() error {
	return delete.Input.TypeCheck()
}
