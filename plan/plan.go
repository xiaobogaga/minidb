package plan

import (
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/parser"
	"github.com/xiaobogaga/minidb/storage"
	"github.com/xiaobogaga/minidb/util"
)

type Plan interface {
	Schema() *storage.TableSchema
	Child() []Plan
	String() string
	TypeCheck() error
	Execute() *storage.RecordBatch
	Reset()
}

type ScanPlan struct {
	Input      *TableScan `json:"table_scan"`
	Name       string     `json:"scan_name"`
	Alias      string     `json:"scan_alias"`
	SchemaName string     `json:"schema_name"`
}

// Return a new schema with a possible new name named by alias
func (scan *ScanPlan) Schema() *storage.TableSchema {
	originalSchema := scan.Input.Schema()
	tableSchema := &storage.TableSchema{}
	for _, column := range originalSchema.Columns {
		field := storage.Field{
			SchemaName: originalSchema.SchemaName(),
			TableName:  originalSchema.TableName(),
			Name:       column.Name,
			TP:         column.TP,
		}
		tableSchema.AppendColumn(field)
	}
	return tableSchema
}

func (scan *ScanPlan) String() string {
	return fmt.Sprintf("ScanPlan: %s as %s", scan.Name, scan.Alias)
}

func (scan *ScanPlan) Child() []Plan {
	return []Plan{scan.Input}
}

func (scan *ScanPlan) TypeCheck() error {
	return scan.Input.TypeCheck()
}

func (scan *ScanPlan) Execute() *storage.RecordBatch {
	// we can return directly.
	return scan.Input.Execute()
}

func (scan *ScanPlan) Reset() {
	scan.Input.Reset()
}

type TableScan struct {
	Name       string `json:"table_name"`
	SchemaName string `json:"schema_name"`
	i          int
}

func (tableScan *TableScan) Schema() *storage.TableSchema {
	db := storage.GetStorage().GetDbInfo(tableScan.SchemaName)
	table := db.GetTable(tableScan.Name)
	return table.TableSchema
}

func (tableScan *TableScan) String() string {
	return fmt.Sprintf("tableScan: %s.%s", tableScan.SchemaName, tableScan.Name)
}

func (tableScan *TableScan) Child() []Plan {
	return nil
}

func (tableScan *TableScan) TypeCheck() error {
	// First, we check whether the database, table exists.
	if !storage.GetStorage().HasSchema(tableScan.SchemaName) {
		return errors.New(fmt.Sprintf("cannot find such schema: '%s'", tableScan.SchemaName))
	}
	if !storage.GetStorage().GetDbInfo(tableScan.SchemaName).HasTable(tableScan.Name) {
		return errors.New(fmt.Sprintf("cannot find such table: '%s'", util.BuildDotString(tableScan.SchemaName, tableScan.Name)))
	}
	return nil
}

var batchSize = 1 << 10

func SetBatchSize(batch int) {
	batchSize = batch
}

func (tableScan *TableScan) Execute() *storage.RecordBatch {
	dbInfo := storage.GetStorage().GetDbInfo(tableScan.SchemaName)
	table := dbInfo.GetTable(tableScan.Name)
	ret := table.FetchData(tableScan.i, batchSize)
	tableScan.i += ret.RowCount()
	return ret
}

func (tableScan *TableScan) Reset() {
	tableScan.i = 0
}

type JoinPlan struct {
	LeftPlan   Plan            `json:"left"`
	JoinType   parser.JoinType `json:"type"`
	RightPlan  Plan            `json:"right"`
	LeftBatch  *storage.RecordBatch
	RightBatch *storage.RecordBatch
	Expr       Expr
}

func NewJoinPlan(left, right Plan, tp parser.JoinType) *JoinPlan {
	return &JoinPlan{
		LeftPlan:  left,
		JoinType:  tp,
		RightPlan: right,
	}
}

func (join *JoinPlan) Schema() *storage.TableSchema {
	leftSchema := join.LeftPlan.Schema()
	rightSchema := join.RightPlan.Schema()
	mergedSchema, _ := leftSchema.Merge(rightSchema)
	return mergedSchema
}

func (join *JoinPlan) String() string {
	return fmt.Sprintf("Join(%s, %s, %s)\n", joinTypeToString(join.JoinType), join.LeftPlan, join.RightPlan)
}

func (join *JoinPlan) Child() []Plan {
	return []Plan{join.LeftPlan, join.RightPlan}
}

func (join *JoinPlan) TypeCheck() error {
	err := join.LeftPlan.TypeCheck()
	if err != nil {
		return err
	}
	return join.RightPlan.TypeCheck()
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

func (join *JoinPlan) Execute() (ret *storage.RecordBatch) {
	if join.LeftBatch == nil {
		join.LeftBatch = join.LeftPlan.Execute()
	}
	if join.RightBatch == nil {
		join.RightBatch = join.RightPlan.Execute()
	}
	switch join.JoinType {
	case parser.LeftOuterJoin:
		if join.LeftBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch, join.LeftPlan.Schema(), join.Schema())
		join.RightBatch = join.RightPlan.Execute()
		if join.RightBatch == nil {
			join.LeftBatch = join.LeftPlan.Execute()
			join.RightPlan.Reset()
		}
	case parser.RightOuterJoin:
		if join.RightBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch, join.LeftPlan.Schema(), join.Schema())
		join.LeftBatch = join.LeftPlan.Execute()
		if join.LeftBatch == nil {
			join.RightBatch = join.RightPlan.Execute()
			join.LeftPlan.Reset()
		}
	case parser.InnerJoin:
		if join.LeftBatch == nil || join.RightBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch, join.LeftPlan.Schema(), join.Schema())
		join.RightBatch = join.RightPlan.Execute()
		if join.RightBatch == nil {
			join.LeftBatch = join.LeftPlan.Execute()
			join.RightPlan.Reset()
		}
	}
	return ret
}

func (join JoinPlan) Reset() {
	join.LeftBatch = nil
	join.RightBatch = nil
	join.LeftPlan.Reset()
	join.RightPlan.Reset()
}

// For where where_condition
type SelectionPlan struct {
	Input Plan `json:"select_input"`
	Expr  Expr `json:"where"`
}

func (sel *SelectionPlan) Schema() *storage.TableSchema {
	// The schema is the same as the original schema
	return sel.Input.Schema()
}

func (sel *SelectionPlan) String() string {
	return fmt.Sprintf("SelectionPlan: %s where %s", sel.Input, sel.Expr)
}

func (sel *SelectionPlan) Child() []Plan {
	return []Plan{sel.Input}
}

func (sel *SelectionPlan) TypeCheck() error {
	err := sel.Input.TypeCheck()
	if err != nil {
		return err
	}
	err = sel.Expr.TypeCheck()
	if err != nil {
		return err
	}
	// Note: doesn't allow group by in where clause.
	if sel.Expr.HasGroupFunc() {
		return errors.New("invalid use of group function")
	}
	f := sel.Expr.toField()
	if f.TP.Name != storage.Bool {
		return errors.New(fmt.Sprintf("%s doesn't return bool value", sel.Expr.String()))
	}
	return nil
}

func GetFieldsFromSchema(schema *storage.TableSchema) (ret []storage.Field) {
	ret = append(ret, schema.Columns...)
	return
}

func MakeEmptyRecordBatchFromSchema(schema *storage.TableSchema) *storage.RecordBatch {
	fields := GetFieldsFromSchema(schema)
	ret := &storage.RecordBatch{
		Fields:  fields,
		Records: make([]*storage.ColumnVector, len(fields)),
	}
	for i, f := range fields {
		ret.Records[i] = &storage.ColumnVector{Field: f}
	}
	return ret
}

func (sel *SelectionPlan) Execute() (ret *storage.RecordBatch) {
	i := 0
	for i < batchSize {
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
	}
	return
}

func (sel *SelectionPlan) Reset() {
	sel.Input.Reset()
}

// The typeCheck for orderBy and Having are different.

// orderBy orderByExpr
type OrderByPlan struct {
	Input   Plan        `json:"order_input"`
	OrderBy OrderByExpr `json:"order_by"`
	IsAggr  bool        `json:"is_aggr"`
	data    *storage.RecordBatch
	index   int
}

func (orderBy *OrderByPlan) Schema() *storage.TableSchema {
	// Should be the same as Expr
	return orderBy.Input.Schema()
}

func (orderBy *OrderByPlan) String() string {
	return fmt.Sprintf("OrderByPlan: %s orderBy %s", orderBy.Input, orderBy.OrderBy)
}

func (orderBy *OrderByPlan) Child() []Plan {
	return []Plan{orderBy.Input}
}

func (orderBy *OrderByPlan) TypeCheck() error {
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
	have, ok := orderBy.Input.(*HavingPlan)
	if ok {
		return orderBy.OrderBy.AggrTypeCheck(have.Input.GroupByExpr)
	}
	return orderBy.OrderBy.AggrTypeCheck(orderBy.Input.(*GroupByPlan).GroupByExpr)
}

func (orderBy *OrderByPlan) Execute() *storage.RecordBatch {
	if orderBy.data == nil {
		orderBy.InitializeAndSort()
	}
	if orderBy.data == nil {
		return nil
	}
	ret := orderBy.data.Slice(orderBy.index, batchSize)
	orderBy.index += batchSize
	return ret
}

func (orderBy *OrderByPlan) InitializeAndSort() {
	if orderBy.data != nil {
		return
	}
	batch := orderBy.Input.Execute()
	if batch == nil {
		return
	}
	ret := MakeEmptyRecordBatchFromSchema(orderBy.Schema())
	for batch != nil {
		ret.Append(batch)
		batch = orderBy.Input.Execute()
	}
	columnVector := orderBy.OrderBy.Evaluate(ret)
	ret.OrderBy(columnVector)
	orderBy.data = ret
}

func (orderBy *OrderByPlan) Reset() {
	orderBy.Input.Reset()
	orderBy.data = nil
	orderBy.index = 0
}

type ProjectionPlan struct {
	Input Plan     `json:"projection_input"`
	Exprs []AsExpr `json:"exprs"`
	// ret   *storage.RecordBatch
}

// We don't support alias for now.
func (proj *ProjectionPlan) Schema() *storage.TableSchema {
	if len(proj.Exprs) == 0 {
		return proj.Input.Schema()
	}
	// the proj can be: select a1.b, a2.b from a1, a2;
	// and the inputSchema can be either:
	// * a pure single table schema.
	// * a joined table schema with multiple sub tables internal.
	table := &storage.TableSchema{
		Columns: []storage.Field{storage.RowIndexField("", "")},
	}
	for _, expr := range proj.Exprs {
		f := expr.toField()
		table.AppendColumn(f)
	}
	return table
}

func (proj *ProjectionPlan) String() string {
	return fmt.Sprintf("proj: %s", proj.Input)
}

func (proj *ProjectionPlan) Child() []Plan {
	return []Plan{proj.Input}
}

func (proj *ProjectionPlan) TypeCheck() error {
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
	// For full group by type check.
	hasGroupBy, nonGroupBy := false, false
	for _, expr := range proj.Exprs {
		temp := expr.HasGroupFunc()
		hasGroupBy = temp
		nonGroupBy = !temp
	}
	if hasGroupBy && nonGroupBy {
		return errors.New("doesn't mix groupBy and nonGroupBy expressions")
	}
	return nil
}

func (proj *ProjectionPlan) Execute() *storage.RecordBatch {
	if proj.IsAggr() {
		return proj.ExecuteAccumulate()
	}
	records := proj.Input.Execute()
	if records == nil {
		return nil
	}
	ret := MakeEmptyRecordBatchFromSchema(proj.Schema())
	for i, expr := range proj.Exprs {
		colVector := expr.Evaluate(records)
		// Note: the row index column
		ret.SetColumnValue(i+1, colVector)
	}
	if len(proj.Exprs) == 0 {
		// Must be select all.
		return records
	}
	// Now we copy the row index.
	ret.Records[0].Appends(records.Records[0])
	return ret
}

// For query like: select sum(id) from test1;
func (proj *ProjectionPlan) ExecuteAccumulate() (ret *storage.RecordBatch) {
	records := proj.Input.Execute()
	if records == nil {
		return nil
	}
	for records != nil {
		for i := 0; i < records.RowCount(); i++ {
			for _, expr := range proj.Exprs {
				expr.Accumulate(i, records)
			}
		}
		records = proj.Input.Execute()
	}
	ret = MakeEmptyRecordBatchFromSchema(proj.Schema())
	ret.Records[0].Append(storage.EncodeInt(0))
	for i, expr := range proj.Exprs {
		value := expr.AccumulateValue()
		ret.Records[i+1].Append(value)
	}
	return ret
}

func (proj *ProjectionPlan) Reset() {
	proj.Input.Reset()
}

func (proj *ProjectionPlan) IsAggr() bool {
	for _, expr := range proj.Exprs {
		if expr.HasGroupFunc() {
			return true
		}
	}
	return false
}

type LimitPlan struct {
	Input  Plan `json:"limit_input"`
	Count  int  `json:"count"`
	Offset int  `json:"offset"`
	Index  int
}

func (limit *LimitPlan) Schema() *storage.TableSchema {
	return limit.Input.Schema()
}

func (limit *LimitPlan) String() string {
	return fmt.Sprintf("LimitPlan: %s limit %d %d", limit.Input, limit.Count, limit.Offset)
}

func (limit *LimitPlan) Child() []Plan {
	return []Plan{limit.Input}
}

func (limit *LimitPlan) TypeCheck() error {
	return limit.Input.TypeCheck()
}

func (limit *LimitPlan) Execute() *storage.RecordBatch {
	if limit.Count <= 0 || limit.Index-limit.Offset >= limit.Count {
		return nil
	}
	batch := limit.Input.Execute()
	if batch == nil {
		return nil
	}
	// Move index to close to offset first.
	for batch != nil && limit.Index+batch.RowCount() <= limit.Offset {
		limit.Index += batch.RowCount()
		batch = limit.Input.Execute()
	}
	// Doesn't have data starting from the offset.
	if batch == nil {
		limit.Index = limit.Offset + limit.Count // mark all data is consumed.
		return nil
	}
	startIndex := 0
	if limit.Index < limit.Offset {
		startIndex = limit.Offset - limit.Index
	}
	size := batch.RowCount()
	if limit.Index+size >= (limit.Offset + limit.Count) {
		size = limit.Count + limit.Offset - startIndex - limit.Index
	}
	// ret.Copy(ret, startIndex, size)
	ret := batch.Slice(startIndex, size)
	limit.Index += batch.RowCount()
	return ret
}

func (limit *LimitPlan) Reset() {
	limit.Input.Reset()
	limit.Index = 0
}
