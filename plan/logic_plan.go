package plan

import (
	"fmt"
	"simpleDb/ast"
)

type Schema struct {
	FieldMap map[string]Field
	Name     string
	Alias    string
}

type RecordBatch struct {
	Fields  map[string]Field
	Records map[string]ColumnVector
}

type Field struct {
	TP   FieldTP
	Name string
}

// A column of field.
type ColumnVector interface{}

type FieldTP string

type LogicPlan interface {
	Schema() Schema
	Child() []LogicPlan
	String() string
}

type ScanLogicPlan struct {
	Input       LogicPlan
	Name        string
	Alias       string
	Projections []string
}

func (scan ScanLogicPlan) Schema() Schema {
	originalSchema := scan.Input.Schema()
	ret := Schema{
		FieldMap: map[string]Field{},
		Name:     scan.Name,
		Alias:    scan.Alias,
	}
	if scan.Name == "" {
		scan.Name = originalSchema.Name
	}
	if scan.Alias == "" {
		scan.Alias = originalSchema.Alias
	}
	for _, projection := range scan.Projections {
		field, ok := originalSchema.FieldMap[projection]
		if !ok {
			panic(fmt.Sprintf("cannot find such projection: %s", projection))
		}
		ret.FieldMap[projection] = field
	}
	return ret
}

func (scan ScanLogicPlan) String() string {
	return fmt.Sprintf("ScanLogicPlan: %s as %s", scan.Name, scan.Alias)
}
func (scan ScanLogicPlan) Child() []LogicPlan {
	return []LogicPlan{scan.Input}
}

// For where where_condition
type SelectionLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func (sel SelectionLogicPlan) Schema() Schema {
	// The schema is the same as the original schema
	return sel.Input.Schema()
}

func (sel SelectionLogicPlan) String() string {
	return fmt.Sprintf("SelectionLogicPlan: %s where %s", sel.Input, sel.Expr)
}
func (sel SelectionLogicPlan) Child() []LogicPlan {
	return []LogicPlan{sel.Input}
}

// For groupBy exprs.
type GroupByLogicPlan struct {
	Input       LogicPlan
	GroupByExpr []LogicExpr
}

func (groupBy GroupByLogicPlan) Schema() Schema {
	// should be the same as the input schema
	return groupBy.Input.Schema()
}

func (groupBy GroupByLogicPlan) String() string {
	return fmt.Sprintf("GroupByLogicPlan: %s groupBy %s", groupBy.Input, groupBy.GroupByExpr)
}

func (groupBy GroupByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{groupBy.Input}
}

// For Having condition
type HavingLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func (having HavingLogicPlan) Schema() Schema {
	// Should be the same schema as Input.
	return having.Input.Schema()
}

func (having HavingLogicPlan) String() string {
	return fmt.Sprintf("HavingLogicPlan: %s having %s", having.Input, having.Expr)
}

func (having HavingLogicPlan) Child() []LogicPlan {
	return []LogicPlan{having.Input}
}

// orderBy orderByExpr
type OrderByLogicPlan struct {
	Input LogicPlan
	Expr  OrderedExpr
}

func (orderBy OrderByLogicPlan) Schema() Schema {
	// Should be the same as Input
	return orderBy.Input.Schema()
}

func (orderBy OrderByLogicPlan) String() string {
	return fmt.Sprintf("OrderByLogicPlan: %s orderBy %s", orderBy.Input, orderBy.Expr)
}

func (orderBy OrderByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{orderBy.Input}
}

type AggregateLogicScan struct{}

func (aggr AggregateLogicScan) Schema() Schema     {}
func (aggr AggregateLogicScan) String() string     {}
func (aggr AggregateLogicScan) Child() []LogicPlan {}

type ProjectionLogicPlan struct {
	Input LogicPlan
	Exprs []LogicExpr
}

func (proj ProjectionLogicPlan) Schema() Schema {}

func (proj ProjectionLogicPlan) String() string {}

func (proj ProjectionLogicPlan) Child() []LogicPlan {}

type JoinLogicPlan struct {
	LeftLogicPlan  LogicPlan
	JoinType       ast.JoinType
	RightLogicPlan LogicPlan
}

type JoinType int

const (
	LeftJoin JoinType = 0
)

func (join JoinLogicPlan) Schema() Schema {
	leftSchema := join.LeftLogicPlan.Schema()
	rightSchema := join.RightLogicPlan.Schema()

}

func (join JoinLogicPlan) String() string {
	return fmt.Sprintf("Join(%s, %s, %s)\n", joinTypeToString(join.JoinType), join.LeftLogicPlan, join.RightLogicPlan)
}

func (join JoinLogicPlan) Child() []LogicPlan {
	return []LogicPlan{join.LeftLogicPlan, join.RightLogicPlan}
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

type LimitLogicPlan struct {
	Input  LogicPlan
	Count  int
	Offset int
}

func (limit LimitLogicPlan) Schema() Schema {
	return limit.Input.Schema()
}

func (limit LimitLogicPlan) String() string {
	return fmt.Sprintf("LimitLogicPlan: %s limit %d %d", limit.Input, limit.Count, limit.Offset)
}
func (limit LimitLogicPlan) Child() []LogicPlan {
	return []LogicPlan{limit.Input}
}
