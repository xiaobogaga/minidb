package plan

import (
	"errors"
	"fmt"
	"simpleDb/ast"
	"simpleDb/storage"
)

type LogicPlan interface {
	Schema() storage.Schema
	Child() []LogicPlan
	String() string
	TypeCheck() error
	Execute() storage.RecordBatch
}

type ScanLogicPlan struct {
	Input      LogicPlan
	Name       string
	Alias      string
	SchemaName string
	// Projections []string
}

func (scan ScanLogicPlan) Schema() storage.Schema {
	originalSchema := scan.Input.Schema()
	ret := storage.Schema{
		FieldMap: originalSchema.FieldMap,
		// TableName:  scan.Name,
		// Alias:      scan.Alias,
		// SchemaName: scan.SchemaName,
	}
	return ret
}

func (scan ScanLogicPlan) String() string {
	return fmt.Sprintf("ScanLogicPlan: %s as %s", scan.Name, scan.Alias)
}

func (scan ScanLogicPlan) Child() []LogicPlan {
	return []LogicPlan{scan.Input}
}

func (scan ScanLogicPlan) TypeCheck() error {
	return scan.Input.TypeCheck()
}

func (scan ScanLogicPlan) Execute() storage.RecordBatch {
	// we can return directly.
	return scan.Input.Execute()
}

type TableScan struct {
	Name       string
	SchemaName string
	i          int
}

func (tableScan TableScan) Schema() storage.Schema {
	db := storage.GetStorage().GetDbInfo(tableScan.SchemaName)
	table := db.GetTable(tableScan.Name)
	return table.Schema
}

func (tableScan TableScan) String() string {
	return fmt.Sprintf("tableScan: %s.%s", tableScan.Schema, tableScan.Name)
}

func (tableScan TableScan) Child() []LogicPlan {
	return nil
}

func (tableScan TableScan) TypeCheck() error {
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

func (tableScan TableScan) Execute() storage.RecordBatch {
	dbInfo := storage.GetStorage().GetDbInfo(tableScan.SchemaName)
	table := dbInfo.GetTable(tableScan.Name)
	return table.FetchData(tableScan.i, BatchSize)
}

// For where where_condition
type SelectionLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func (sel SelectionLogicPlan) Schema() storage.Schema {
	// The schema is the same as the original schema
	return sel.Input.Schema()
}

func (sel SelectionLogicPlan) String() string {
	return fmt.Sprintf("SelectionLogicPlan: %s where %s", sel.Input, sel.Expr)
}

func (sel SelectionLogicPlan) Child() []LogicPlan {
	return []LogicPlan{sel.Input}
}

func (sel SelectionLogicPlan) TypeCheck() error {
	err := sel.Input.TypeCheck()
	if err != nil {
		return err
	}
	return sel.Expr.TypeCheck(sel.Input)
}

func (sel SelectionLogicPlan) Execute() storage.RecordBatch {
	ret := sel.Input.Execute()
	sel.Expr.Evaluate()
}

// For groupBy exprs.
type GroupByLogicPlan struct {
	Input       LogicPlan
	GroupByExpr []LogicExpr
}

func (groupBy GroupByLogicPlan) Schema() storage.Schema {
	// should be the same as the input schema
	return groupBy.Input.Schema()
}

func (groupBy GroupByLogicPlan) String() string {
	return fmt.Sprintf("GroupByLogicPlan: %s groupBy %s", groupBy.Input, groupBy.GroupByExpr)
}

func (groupBy GroupByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{groupBy.Input}
}

func (groupBy GroupByLogicPlan) TypeCheck() error {
	err := groupBy.Input.TypeCheck()
	if err != nil {
		return err
	}
	for _, expr := range groupBy.GroupByExpr {
		err = expr.TypeCheck((groupBy.Input))
		if err != nil {
			return err
		}
	}
	return nil
}

func (groupBy GroupByLogicPlan) Execute() storage.RecordBatch {}

// For Having condition
type HavingLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func (having HavingLogicPlan) Schema() storage.Schema {
	// Should be the same schema as Input.
	return having.Input.Schema()
}

func (having HavingLogicPlan) String() string {
	return fmt.Sprintf("HavingLogicPlan: %s having %s", having.Input, having.Expr)
}

func (having HavingLogicPlan) Child() []LogicPlan {
	return []LogicPlan{having.Input}
}

func (having HavingLogicPlan) TypeCheck() error {
	err := having.Input.TypeCheck()
	if err != nil {
		return err
	}
	return having.Expr.TypeCheck(having.Input)
}

func (having HavingLogicPlan) Execute() storage.RecordBatch {}

// orderBy orderByExpr
type OrderByLogicPlan struct {
	Input   LogicPlan
	OrderBy OrderedLogicExpr
}

func (orderBy OrderByLogicPlan) Schema() storage.Schema {
	// Should be the same as Input
	return orderBy.Input.Schema()
}

func (orderBy OrderByLogicPlan) String() string {
	return fmt.Sprintf("OrderByLogicPlan: %s orderBy %s", orderBy.Input, orderBy.OrderBy)
}

func (orderBy OrderByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{orderBy.Input}
}

func (orderBy OrderByLogicPlan) TypeCheck() error {
	err := orderBy.Input.TypeCheck()
	if err != nil {
		return err
	}
	return orderBy.OrderBy.TypeCheck(orderBy.Input)
}

func (orderBy OrderByLogicPlan) Execute() storage.RecordBatch {}

type AggregateLogicScan struct{}

func (aggr AggregateLogicScan) Schema() storage.Schema {}
func (aggr AggregateLogicScan) String() string         {}
func (aggr AggregateLogicScan) Child() []LogicPlan     {}
func (aggr AggregateLogicScan) TypeCheck() error {

}

type ProjectionLogicPlan struct {
	Input LogicPlan
	Exprs []LogicExpr
}

// Todo
func (proj ProjectionLogicPlan) Schema() storage.Schema {
	inputSchema := proj.Input.Schema()
	// the proj can be: select a1.b, a2.b from a1, a2;
	// and the inputSchame can be either:
	// * a pure single table schame.
	// * a joined table schema with multiple sub tables internal.
	ret := storage.Schema{
		// SchemaName: inputSchema.SchemaName,
		// TableName: inputSchema.TableName,
		// Alias:  inputSchema.Alias,
		FieldMap: map[string]map[string]map[string]Field{},
	}
	for _, expr := range proj.Exprs {
		f := expr.toField(proj.Input)
		table, ok := ret[f.TableName]
		if !ok {
			table = map[string]Field{}
			ret[f.TableName] = table
		}
		table[f.Name] = f
	}
	return ret
}

func (proj ProjectionLogicPlan) String() string {
	// Todo
	return fmt.Sprintf("proj: %s", proj.Input)
}

func (proj ProjectionLogicPlan) Child() []LogicPlan {
	return []LogicPlan{proj.Input}
}

func (proj ProjectionLogicPlan) TypeCheck() error {
	err := proj.Input.TypeCheck()
	if err != nil {
		return err
	}
	for _, expr := range proj.Exprs {
		err = expr.TypeCheck(proj.Input)
		if err != nil {
			return err
		}
	}
	return nil
}

func (proj ProjectionLogicPlan) Execute() storage.RecordBatch {}

type JoinLogicPlan struct {
	LeftLogicPlan  LogicPlan
	JoinType       ast.JoinType
	RightLogicPlan LogicPlan
}

type JoinType int

const (
	LeftJoin JoinType = 0
)

func (join JoinLogicPlan) Schema() storage.Schema {
	leftSchema := join.LeftLogicPlan.Schema()
	rightSchema := join.RightLogicPlan.Schema()
	return leftSchema.Merge(rightSchema)
}

func (join JoinLogicPlan) String() string {
	return fmt.Sprintf("Join(%s, %s, %s)\n", joinTypeToString(join.JoinType), join.LeftLogicPlan, join.RightLogicPlan)
}

func (join JoinLogicPlan) Child() []LogicPlan {
	return []LogicPlan{join.LeftLogicPlan, join.RightLogicPlan}
}

func (join JoinLogicPlan) TypeCheck() error {
	err := join.LeftLogicPlan.TypeCheck()
	if err != nil {
		return err
	}
	return join.RightLogicPlan.TypeCheck()
}

func joinTypeToString(joinType ast.JoinType) string {
	switch joinType {
	case ast.InnerJoin:
		return "innerJoin"
	case ast.LeftOuterJoin:
		return "leftOuterJoin"
	case ast.RightOuterJoin:
		return "rightOuterJoin"
	default:
		return ""
	}
}

func (join JoinLogicPlan) Execute() storage.RecordBatch {

}

type LimitLogicPlan struct {
	Input  LogicPlan
	Count  int
	Offset int
}

func (limit LimitLogicPlan) Schema() storage.Schema {
	return limit.Input.Schema()
}

func (limit LimitLogicPlan) String() string {
	return fmt.Sprintf("LimitLogicPlan: %s limit %d %d", limit.Input, limit.Count, limit.Offset)
}

func (limit LimitLogicPlan) Child() []LogicPlan {
	return []LogicPlan{limit.Input}
}

func (limit LimitLogicPlan) TypeCheck() error {
	return limit.Input.TypeCheck()
}

func (limit LimitLogicPlan) Execute() storage.RecordBatch {}
