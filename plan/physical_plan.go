package plan

type PhysicalPlan interface {
	Execute() []RecordBatch
}

type TableScanExec struct{}

type SelectionExec struct{}

type ProjectionExec struct{}

type HashAggregateExec struct{}

type HashJoinExec struct{}
