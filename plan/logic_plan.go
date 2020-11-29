package plan

import (
	"fmt"
	"simpleDb/ast"
)

type Schema struct {
	FieldMap map[string]Field
	Name     string
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
	dataSource  interface{}
	name        string
	alias       string
	projections []string
}

func NewScanLogicPlan(name, alias string) ScanLogicPlan {
	// Todo
	return ScanLogicPlan{
		name:  name,
		alias: alias,
	}
}

func (scan ScanLogicPlan) Schema() Schema {}
func (scan ScanLogicPlan) String() string {
	return fmt.Sprintf("ScanLogicPlan: %s as %s", scan.name, scan.alias)
}
func (scan ScanLogicPlan) Child() []LogicPlan {}

type SelectionLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func NewSelectionLogicScan() SelectionLogicPlan {}

func (sel SelectionLogicPlan) Schema() Schema     {}
func (sel SelectionLogicPlan) String() string     {}
func (sel SelectionLogicPlan) Child() []LogicPlan {}

type GroupByLogicPlan struct {
	Input       LogicPlan
	GroupByExpr []LogicExpr
}

func (groupBy GroupByLogicPlan) Schema() Schema     {}
func (groupBy GroupByLogicPlan) String() string     {}
func (groupBy GroupByLogicPlan) Child() []LogicPlan {}

type HavingLogicPlan struct {
	Input LogicPlan
	Expr  LogicExpr
}

func (having HavingLogicPlan) Schema() Schema     {}
func (having HavingLogicPlan) String() string     {}
func (having HavingLogicPlan) Child() []LogicPlan {}

type OrderByLogicPlan struct {
	Input LogicPlan
	Expr  OrderedExpr
}

func (orderBy OrderByLogicPlan) Schema() Schema     {}
func (orderBy OrderByLogicPlan) String() string     {}
func (orderBy OrderByLogicPlan) Child() []LogicPlan {}

type AggregateLogicScan struct{}

func NewAggregateLogicScan() AggregateLogicScan {}

func (aggr AggregateLogicScan) Schema() Schema     {}
func (aggr AggregateLogicScan) String() string     {}
func (aggr AggregateLogicScan) Child() []LogicPlan {}

type ProjectionLogicPlan struct {
	Input LogicPlan
	Exprs []LogicExpr
}

func NewProjectionLogicPlan() ProjectionLogicPlan {}

func (proj ProjectionLogicPlan) Schema() Schema     {}
func (proj ProjectionLogicPlan) String() string     {}
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

func NewJoinLogicPlan() JoinLogicPlan {}

func (join JoinLogicPlan) Schema() Schema {}
func (join JoinLogicPlan) String() string {
	return fmt.Sprintf("Join(%s, %s, %s)\n", joinTypeToString(join.JoinType), join.LeftLogicPlan, join.RightLogicPlan)
}
func (join JoinLogicPlan) Child() []LogicPlan {}

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

func (limit LimitLogicPlan) Schema() Schema     {}
func (limit LimitLogicPlan) String() string     {}
func (limit LimitLogicPlan) Child() []LogicPlan {}
