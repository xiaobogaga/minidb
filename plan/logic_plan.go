package plan

import (
	"errors"
	"fmt"
	"minidb/parser"
	"minidb/storage"
)

type LogicPlan interface {
	Schema() *storage.TableSchema
	Child() []LogicPlan
	String() string
	TypeCheck() error
	Execute() *storage.RecordBatch
	Reset()
}

type ScanLogicPlan struct {
	Input      *TableScan `json:"table_scan"`
	Name       string     `json:"scan_name"`
	Alias      string     `json:"scan_alias"`
	SchemaName string     `json:"schema_name"`
}

// Return a new schema with a possible new name named by alias
func (scan *ScanLogicPlan) Schema() *storage.TableSchema {
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

type JoinLogicPlan struct {
	LeftLogicPlan  LogicPlan       `json:"left"`
	JoinType       parser.JoinType `json:"type"`
	RightLogicPlan LogicPlan       `json:"right"`
	LeftBatch      *storage.RecordBatch
	RightBatch     *storage.RecordBatch
	Expr           LogicExpr
}

func NewJoinLogicPlan(left, right LogicPlan, tp parser.JoinType) *JoinLogicPlan {
	return &JoinLogicPlan{
		LeftLogicPlan:  left,
		JoinType:       tp,
		RightLogicPlan: right,
	}
}

func (join *JoinLogicPlan) Schema() *storage.TableSchema {
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
	if join.RightBatch == nil {
		join.RightBatch = join.RightLogicPlan.Execute()
	}
	switch join.JoinType {
	case parser.LeftOuterJoin:
		if join.LeftBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch, join.LeftLogicPlan.Schema(), join.Schema())
		join.RightBatch = join.RightLogicPlan.Execute()
		if join.RightBatch == nil {
			join.LeftBatch = join.LeftLogicPlan.Execute()
			join.RightLogicPlan.Reset()
		}
	case parser.RightOuterJoin:
		if join.RightBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch, join.LeftLogicPlan.Schema(), join.Schema())
		join.LeftBatch = join.LeftLogicPlan.Execute()
		if join.LeftBatch == nil {
			join.RightBatch = join.RightLogicPlan.Execute()
			join.LeftLogicPlan.Reset()
		}
	case parser.InnerJoin:
		if join.LeftBatch == nil || join.RightBatch == nil {
			return nil
		}
		ret = join.LeftBatch.Join(join.RightBatch, join.LeftLogicPlan.Schema(), join.Schema())
		join.RightBatch = join.RightLogicPlan.Execute()
		if join.RightBatch == nil {
			join.LeftBatch = join.LeftLogicPlan.Execute()
			join.RightLogicPlan.Reset()
		}
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
	Input LogicPlan `json:"select_input"`
	Expr  LogicExpr `json:"where"`
}

func (sel *SelectionLogicPlan) Schema() *storage.TableSchema {
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
	// Note: doesn't allow group by in where clause.
	if sel.Expr.HasGroupFunc() {
		return errors.New("invalid use of group function")
	}
	f := sel.Expr.toField()
	if f.TP != storage.Bool {
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

func (sel *SelectionLogicPlan) Execute() (ret *storage.RecordBatch) {
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

func (sel *SelectionLogicPlan) Reset() {
	sel.Input.Reset()
}

// The typeCheck for orderBy and Having are different.

// orderBy orderByExpr
type OrderByLogicPlan struct {
	Input   LogicPlan        `json:"order_input"`
	OrderBy OrderByLogicExpr `json:"order_by"`
	IsAggr  bool             `json:"is_aggr"`
	data    *storage.RecordBatch
	index   int
}

func (orderBy *OrderByLogicPlan) Schema() *storage.TableSchema {
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
	have, ok := orderBy.Input.(*HavingLogicPlan)
	if ok {
		return orderBy.OrderBy.AggrTypeCheck(have.Input.GroupByExpr)
	}
	return orderBy.OrderBy.AggrTypeCheck(orderBy.Input.(*GroupByLogicPlan).GroupByExpr)
}

func (orderBy *OrderByLogicPlan) Execute() *storage.RecordBatch {
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

func (orderBy *OrderByLogicPlan) InitializeAndSort() {
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

func (orderBy *OrderByLogicPlan) Reset() {
	orderBy.Input.Reset()
	orderBy.data = nil
	orderBy.index = 0
}

type ProjectionLogicPlan struct {
	Input LogicPlan     `json:"projection_input"`
	Exprs []AsLogicExpr `json:"exprs"`
	// ret   *storage.RecordBatch
}

// We don't support alias for now.
func (proj *ProjectionLogicPlan) Schema() *storage.TableSchema {
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

func (proj *ProjectionLogicPlan) Execute() *storage.RecordBatch {
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
func (proj *ProjectionLogicPlan) ExecuteAccumulate() (ret *storage.RecordBatch) {
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

func (proj *ProjectionLogicPlan) Reset() {
	proj.Input.Reset()
}

func (proj *ProjectionLogicPlan) IsAggr() bool {
	for _, expr := range proj.Exprs {
		if expr.HasGroupFunc() {
			return true
		}
	}
	return false
}

type LimitLogicPlan struct {
	Input  LogicPlan `json:"limit_input"`
	Count  int       `json:"count"`
	Offset int       `json:"offset"`
	Index  int
}

func (limit *LimitLogicPlan) Schema() *storage.TableSchema {
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

func (limit *LimitLogicPlan) Reset() {
	limit.Input.Reset()
	limit.Index = 0
}
