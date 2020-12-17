package plan

import (
	"errors"
	"fmt"
	"simpleDb/parser"
	"simpleDb/storage"
)

type LogicPlan interface {
	Schema() storage.Schema
	Child() []LogicPlan
	String() string
	TypeCheck() error
	Execute() *storage.RecordBatch
	Reset()
}

type ScanLogicPlan struct {
	Input      *TableScan
	Name       string
	Alias      string
	SchemaName string
}

// Return a new schema with a possible new name named by alias
func (scan *ScanLogicPlan) Schema() storage.Schema {
	originalSchema := scan.Input.Schema()
	tableSchema := storage.SingleTableSchema{
		TableName:  scan.Alias,
		SchemaName: scan.SchemaName,
	}
	for _, column := range originalSchema.Tables[0].Columns {
		tableSchema.Columns = append(tableSchema.Columns, storage.Field{
			SchemaName: scan.SchemaName,
			TableName:  scan.Alias,
			Name:       column.Name,
			TP:         column.TP,
		})
	}
	return storage.Schema{Tables: []storage.SingleTableSchema{tableSchema}}
}

func (scan *ScanLogicPlan) String() string {
	return fmt.Sprintf("ScanLogicPlan: %s as %s", scan.Name, scan.Alias)
}

func (scan *ScanLogicPlan) Child() []LogicPlan {
	return []LogicPlan{scan.Input}
}

func (scan *ScanLogicPlan) TypeCheck() error {
	return scan.Input.TypeCheck()
}

func (scan *ScanLogicPlan) Execute() *storage.RecordBatch {
	// we can return directly.
	return scan.Input.Execute()
}

func (scan *ScanLogicPlan) Reset() {
	scan.Input.Reset()
}

type TableScan struct {
	Name       string
	SchemaName string
	i          int
}

func (tableScan *TableScan) Schema() storage.Schema {
	db := storage.GetStorage().GetDbInfo(tableScan.SchemaName)
	table := db.GetTable(tableScan.Name)
	return table.Schema
}

func (tableScan *TableScan) String() string {
	return fmt.Sprintf("tableScan: %s.%s", tableScan.Schema, tableScan.Name)
}

func (tableScan *TableScan) Child() []LogicPlan {
	return nil
}

func (tableScan *TableScan) TypeCheck() error {
	// First, we check whether the database, table exists.
	if !storage.GetStorage().HasSchema(tableScan.SchemaName) {
		return errors.New("cannot find such schema")
	}
	if !storage.GetStorage().GetDbInfo(tableScan.SchemaName).HasTable(tableScan.Name) {
		return errors.New("cannot find such table")
	}
	return nil
}

const BatchSize = 1 << 10

func (tableScan *TableScan) Execute() *storage.RecordBatch {
	dbInfo := storage.GetStorage().GetDbInfo(tableScan.SchemaName)
	table := dbInfo.GetTable(tableScan.Name)
	return table.FetchData(tableScan.i, BatchSize)
}

func (tableScan *TableScan) Reset() {
	tableScan.i = 0
}

type JoinLogicPlan struct {
	LeftLogicPlan  LogicPlan
	JoinType       parser.JoinType
	RightLogicPlan LogicPlan
	LeftBatch      *storage.RecordBatch
	RightBatch     *storage.RecordBatch
}

func (join *JoinLogicPlan) Schema() storage.Schema {
	leftSchema := join.LeftLogicPlan.Schema()
	rightSchema := join.RightLogicPlan.Schema()
	mergedSchema, _ := leftSchema.Merge(rightSchema)
	return mergedSchema
}

func (join *JoinLogicPlan) String() string {
	return fmt.Sprintf("Join(%s, %s, %s)\n", joinTypeToString(join.JoinType), join.LeftLogicPlan, join.RightLogicPlan)
}

func (join *JoinLogicPlan) Child() []LogicPlan {
	return []LogicPlan{join.LeftLogicPlan, join.RightLogicPlan}
}

func (join *JoinLogicPlan) TypeCheck() error {
	err := join.LeftLogicPlan.TypeCheck()
	if err != nil {
		return err
	}
	return join.RightLogicPlan.TypeCheck()
}

func joinTypeToString(joinType parser.JoinType) string {
	switch joinType {
	case parser.InnerJoin:
		return "innerJoin"
	case parser.LeftOuterJoin:
		return "leftOuterJoin"
	case parser.RightOuterJoin:
		return "rightOuterJoin"
	default:
		return ""
	}
}

func (join *JoinLogicPlan) Execute() (ret *storage.RecordBatch) {
	if join.LeftBatch == nil {
		join.LeftBatch = join.LeftLogicPlan.Execute()
	}
	if join.RightLogicPlan == nil {
		join.RightBatch = join.RightLogicPlan.Execute()
	}
	switch join.JoinType {
	case parser.LeftOuterJoin:
		if join.LeftBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch)
	case parser.RightOuterJoin:
		if join.RightLogicPlan == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch)
	case parser.InnerJoin:
		if join.LeftBatch == nil || join.RightBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch)
	}
	if ret == nil {
		join.RightBatch = join.RightLogicPlan.Execute()
		if join.RightBatch == nil {
			join.LeftBatch = join.LeftLogicPlan.Execute()
			join.RightLogicPlan.Reset()
		}
		return join.Execute()
	}
	return ret
}

func (join JoinLogicPlan) Reset() {
	join.LeftBatch = nil
	join.RightBatch = nil
	join.LeftLogicPlan.Reset()
	join.RightLogicPlan.Reset()
}

// For where where_condition
type SelectionLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func (sel *SelectionLogicPlan) Schema() storage.Schema {
	// The schema is the same as the original schema
	return sel.Input.Schema()
}

func (sel *SelectionLogicPlan) String() string {
	return fmt.Sprintf("SelectionLogicPlan: %s where %s", sel.Input, sel.Expr)
}

func (sel *SelectionLogicPlan) Child() []LogicPlan {
	return []LogicPlan{sel.Input}
}

func (sel *SelectionLogicPlan) TypeCheck() error {
	err := sel.Input.TypeCheck()
	if err != nil {
		return err
	}
	err = sel.Expr.TypeCheck()
	if err != nil {
		return err
	}
	// Note:
	if sel.Expr.HasGroupFunc() {
		return errors.New("invalid use of group function")
	}
	f := sel.Expr.toField()
	if f.TP != storage.Bool {
		return errors.New(fmt.Sprintf("%s doesn't return bool value", sel.Expr.String()))
	}
	return nil
}

func GetFieldsFromSchema(schema storage.Schema) (ret []storage.Field) {
	for _, tableSchema := range schema.Tables {
		ret = append(ret, tableSchema.Columns...)
	}
	return
}

func MakeEmptyRecordBatchFromSchema(schema storage.Schema) *storage.RecordBatch {
	fields := GetFieldsFromSchema(schema)
	ret := &storage.RecordBatch{
		Fields:  fields,
		Records: make([]storage.ColumnVector, len(fields)),
	}
	for i, f := range fields {
		ret.Records[i].Field = f
	}
	return ret
}

func (sel *SelectionLogicPlan) Execute() (ret *storage.RecordBatch) {
	i := 0
	for i < BatchSize {
		recordBatch := sel.Input.Execute()
		if recordBatch == nil {
			return ret
		}
		if ret == nil {
			ret = MakeEmptyRecordBatchFromSchema(sel.Schema())
		}
		selectedRows := sel.Expr.Evaluate(recordBatch)
		selectedRecords := recordBatch.Filter(selectedRows)
		ret.Append(selectedRecords)
		i += selectedRecords.RowCount()
		//for row := 0; row < selectedRows.Size(); row++ {
		//	if !selectedRows.Bool(row) {
		//		continue
		//	}
		//	// Now row is selected
		//	ret.AppendRecord(recordBatch, row)
		//	i++
		//}
	}
	return
}

func (sel *SelectionLogicPlan) Reset() {
	sel.Input.Reset()
}

// The typeCheck for orderBy and Having are different.

// orderBy orderByExpr
type OrderByLogicPlan struct {
	Input   LogicPlan
	OrderBy OrderByLogicExpr
	IsAggr  bool
	data    *storage.RecordBatch
	index   int
}

func (orderBy *OrderByLogicPlan) Schema() storage.Schema {
	// Should be the same as Expr
	return orderBy.Input.Schema()
}

func (orderBy *OrderByLogicPlan) String() string {
	return fmt.Sprintf("OrderByLogicPlan: %s orderBy %s", orderBy.Input, orderBy.OrderBy)
}

func (orderBy *OrderByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{orderBy.Input}
}

func (orderBy *OrderByLogicPlan) TypeCheck() error {
	err := orderBy.Input.TypeCheck()
	if err != nil {
		return err
	}
	err = orderBy.OrderBy.TypeCheck()
	if err != nil {
		return err
	}
	if !orderBy.IsAggr {
		return nil
	}
	return orderBy.OrderBy.AggrTypeCheck(orderBy.Input.(GroupByLogicPlan).GroupByExpr)
}

func (orderBy *OrderByLogicPlan) Execute() *storage.RecordBatch {
	if orderBy.data == nil {
		orderBy.InitializeAndSort()
	}
	ret := orderBy.data.Slice(orderBy.index, BatchSize)
	orderBy.index += BatchSize
	return ret
}

func (orderBy OrderByLogicPlan) InitializeAndSort() {
	if orderBy.data != nil {
		return
	}
	batch := orderBy.Input.Execute()
	ret := MakeEmptyRecordBatchFromSchema(orderBy.Schema())
	for {
		if batch == nil {
			columnVector := orderBy.OrderBy.Evaluate(ret)
			ret.OrderBy(columnVector)
			break
		}
		batch = orderBy.Input.Execute()
		ret.Append(batch)
	}
	orderBy.data = ret
}

func (orderBy *OrderByLogicPlan) Reset() {
	orderBy.Input.Reset()
	orderBy.data = nil
	orderBy.index = 0
}

type ProjectionLogicPlan struct {
	Input LogicPlan
	Exprs []AsLogicExpr
}

// We don't support alias for now.
func (proj *ProjectionLogicPlan) Schema() storage.Schema {
	if len(proj.Exprs) == 0 {
		return proj.Input.Schema()
	}
	// the proj can be: select a1.b, a2.b from a1, a2;
	// and the inputSchema can be either:
	// * a pure single table schema.
	// * a joined table schema with multiple sub tables internal.
	table := storage.SingleTableSchema{}
	ret := storage.Schema{
		Tables: []storage.SingleTableSchema{table},
		// Name:   "projection",
	}
	for _, expr := range proj.Exprs {
		f := expr.toField()
		table.Columns = append(table.Columns, f)
	}
	return ret
}

func (proj *ProjectionLogicPlan) String() string {
	return fmt.Sprintf("proj: %s", proj.Input)
}

func (proj *ProjectionLogicPlan) Child() []LogicPlan {
	return []LogicPlan{proj.Input}
}

func (proj *ProjectionLogicPlan) TypeCheck() error {
	err := proj.Input.TypeCheck()
	if err != nil {
		return err
	}
	for _, expr := range proj.Exprs {
		err = expr.TypeCheck()
		if err != nil {
			return err
		}
	}
	return nil
}

func (proj *ProjectionLogicPlan) Execute() *storage.RecordBatch {
	ret := MakeEmptyRecordBatchFromSchema(proj.Schema())
	records := proj.Input.Execute()
	for i, expr := range proj.Exprs {
		colVector := expr.Evaluate(records)
		ret.SetColumnValue(i, colVector)
	}
	return ret
}

func (proj *ProjectionLogicPlan) Reset() {
	proj.Input.Reset()
}

type LimitLogicPlan struct {
	Input  LogicPlan
	Count  int
	Offset int // start from 0
	Index  int
}

func (limit *LimitLogicPlan) Schema() storage.Schema {
	return limit.Input.Schema()
}

func (limit *LimitLogicPlan) String() string {
	return fmt.Sprintf("LimitLogicPlan: %s limit %d %d", limit.Input, limit.Count, limit.Offset)
}

func (limit *LimitLogicPlan) Child() []LogicPlan {
	return []LogicPlan{limit.Input}
}

func (limit *LimitLogicPlan) TypeCheck() error {
	return limit.Input.TypeCheck()
}

func (limit *LimitLogicPlan) Execute() *storage.RecordBatch {
	ret := MakeEmptyRecordBatchFromSchema(limit.Schema())
	if limit.Count <= 0 || limit.Index-limit.Offset >= limit.Count {
		return nil
	}
	batch := limit.Execute()
	// Move index to close to offset first.
	for batch != nil && limit.Index+batch.RowCount() < limit.Offset {
		limit.Index += batch.RowCount()
		batch = limit.Execute()
	}
	// Doesn't have data starting from the offset.
	if batch == nil {
		limit.Index = limit.Offset + limit.Count // mark all data is consumed.
		return ret
	}
	startIndex := 0
	if limit.Index < limit.Offset {
		startIndex = limit.Offset - limit.Index - 1
	}
	size := batch.RowCount()
	if limit.Index+size > limit.Count {
		size = limit.Count - (limit.Index - limit.Offset)
	}
	// ret.Copy(ret, startIndex, size)
	return ret.Slice(startIndex, size)
}

func (limit *LimitLogicPlan) Reset() {
	limit.Input.Reset()
	limit.Index = 0
}
