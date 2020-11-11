package plan

type PhysicalExpr interface {
	Evaluate(input RecordBatch)
}

type ColumnPhysicalExpr struct{}

type LiteralPhysicalExpr struct{}

type BinaryPhysicalExpr struct{}

type EQPhysicalExpr struct{}

type AddPhysicalExpr struct{}

type AggregatePhysicalExpr struct{}

// etc...
