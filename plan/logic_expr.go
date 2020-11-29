package plan

type LogicExpr interface {
	toField(input LogicPlan) Field
	String() string
}

// can be a table name or a column name.
// For example: select a.name from people a;
type IdentifierLogicPlan struct{}

type LiteralExpr struct{}

type BinaryExpr struct{}

type BooleanBinaryExpr struct{}

type Eq struct{}

type Neq struct{}

type Gt struct{}

type GtEq struct{}

type Lt struct{}

type LtEq struct{}

type And struct{}

type Or struct{}

// Math expr
type Add struct{}

type AggrExpr struct{}

type OrderedExpr struct {
}
