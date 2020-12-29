package plan

import (
	"errors"
	"fmt"
	"minidb/parser"
	"minidb/storage"
)

// For groupBy exprs.
// HashGroupBy.
type GroupByLogicPlan struct {
	Input       LogicPlan            `json:"group_by_input"`
	GroupByExpr []LogicExpr          `json:"group_by_expr"`
	AggrExprs   []LogicExpr          `json:"aggrs"`
	data        *storage.RecordBatch // All record batch from the input.
	keys        *storage.RecordBatch // The keys from groupBy clause
	retData     *storage.RecordBatch // The data will return by the AggrExprs
	index       int
}

func (groupBy GroupByLogicPlan) Schema() *storage.TableSchema {
	ret := &storage.TableSchema{}
	for _, aggrExpr := range groupBy.AggrExprs {
		f := aggrExpr.toField()
		ret.Columns = append(ret.Columns, f)
	}
	return ret
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
		err = expr.TypeCheck()
		if err != nil {
			return err
		}
		if expr.HasGroupFunc() {
			return errors.New("invalid use of group function")
		}
	}
	// Now we check whether aggrExprs is okay.
	for _, aggrExpr := range groupBy.AggrExprs {
		err = aggrExpr.AggrTypeCheck(groupBy.GroupByExpr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (groupBy GroupByLogicPlan) Execute() *storage.RecordBatch {
	if groupBy.data == nil {
		groupBy.InitializeData()
	}
	ret := groupBy.retData.Slice(groupBy.index, BatchSize)
	groupBy.index += BatchSize
	return ret
}

// GroupBy.
// |---|---|---|  group by col1, col2.
// |---|---|---|           |----|----|
func (groupBy GroupByLogicPlan) InitializeData() {
	if groupBy.data != nil {
		return
	}
	// Load all data from input and calculate the data for the keys.
	groupBy.data = MakeEmptyRecordBatchFromSchema(groupBy.Input.Schema())
	groupBy.keys = &storage.RecordBatch{}
	for _, groupByExpr := range groupBy.GroupByExpr {
		f := groupByExpr.toField()
		groupBy.keys.Fields = append(groupBy.keys.Fields, f)
	}
	for {
		batch := groupBy.Input.Execute()
		if batch == nil {
			break
		}
		groupBy.data.Append(batch)
		// Now we calculate the values of keys.
		for j, groupByExpr := range groupBy.GroupByExpr {
			groupBy.keys.Records[j].Appends(groupByExpr.Evaluate(batch).Values)
		}
	}

	// Here we have all data including keys.
	// Table1(data), Table2(keys) - group by keys.
	// |-|-|-|     , |-|-|-|

	// Now builds accumulators.
	groupBy.retData = MakeEmptyRecordBatchFromSchema(groupBy.Schema())
	keyMap := map[string][]LogicExpr{}
	for i := 0; i < groupBy.keys.RowCount(); i++ {
		key := groupBy.keys.RowKey(i)
		value, ok := keyMap[string(key)]
		if !ok {
			value = groupBy.CloneAggrExpr(false)
			keyMap[string(key)] = value
		}
		for _, expr := range value {
			// Accumulate row i at groupBy.data.
			expr.Accumulate(i, groupBy.data)
		}
	}
	// Now we have accumulate all data. It's time to collect all individual group now.
	for _, values := range keyMap {
		for i, value := range values {
			groupBy.retData.Records[i].Append(value.AccumulateValue())
		}
	}
}

func (groupBy GroupByLogicPlan) CloneAggrExpr(needAccumulator bool) (ret []LogicExpr) {
	ret = make([]LogicExpr, len(groupBy.AggrExprs))
	for i, aggrExpr := range groupBy.AggrExprs {
		ret[i] = aggrExpr.Clone(needAccumulator)
	}
	return ret
}

func (groupBy GroupByLogicPlan) Reset() {
	groupBy.data = nil
	groupBy.keys = nil
	groupBy.retData = nil
	groupBy.index = 0
}

// For Having condition
type HavingLogicPlan struct {
	Input GroupByLogicPlan `json:"having_input"`
	Expr  LogicExpr        `json:"Expr"`
}

func (having HavingLogicPlan) Schema() *storage.TableSchema {
	// Should be the same schema as Expr.
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
	return having.Expr.AggrTypeCheck(having.Input.GroupByExpr)
}

func (having HavingLogicPlan) Execute() (ret *storage.RecordBatch) {
	i := 0
	for i < BatchSize {
		recordBatch := having.Input.Execute()
		if recordBatch == nil {
			return
		}
		if ret == nil {
			ret = MakeEmptyRecordBatchFromSchema(having.Input.Schema())
		}
		selectedRows := having.Expr.Evaluate(recordBatch)
		selectedRecord := recordBatch.Filter(selectedRows)
		ret.Append(selectedRecord)
		i += selectedRecord.RowCount()
		//for row := 0; row < selectedRows.Size(); row ++ {
		//	if !selectedRows.Bool(row) {
		//		continue
		//	}
		//	ret.AppendRecord(recordBatch, row)
		//	i ++
		//}
	}
	return
}

func (having HavingLogicPlan) Reset() {
	having.Input.Reset()
}

func MakeAggreLogicPlan(input LogicPlan, ast *parser.SelectStm) (LogicPlan, error) {
	groupByLogicPlan := makeGroupByLogicPlan(input, ast.Groupby)
	// Having similar to projections for aggregation, the Expr must be either included in the group by Expr.
	// or must be an aggregation function.
	havingLogicPlan := makeHavingLogicPlan(groupByLogicPlan, ast.Having)
	// Order by similar to projections for aggregation, the Expr must be either included in the group by Expr,
	// or must be an aggregation function.
	orderByLogicPlan := makeOrderByLogicPlan(havingLogicPlan, ast.OrderBy, true)
	limitLogicPlan := makeLimitLogicPlan(orderByLogicPlan, ast.LimitStm)
	return limitLogicPlan, limitLogicPlan.TypeCheck()
}

func makeHavingLogicPlan(input GroupByLogicPlan, having parser.HavingStm) LogicPlan {
	if having == nil {
		return input
	}
	return HavingLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(having, input),
	}
}

func makeGroupByLogicPlan(input LogicPlan, groupBy *parser.GroupByStm) GroupByLogicPlan {
	return GroupByLogicPlan{
		Input:       input,
		GroupByExpr: ExprStmsToLogicExprs(*groupBy, input),
	}
}

func ExprStmsToLogicExprs(expressions []*parser.ExpressionStm, input LogicPlan) (ret []LogicExpr) {
	for _, expr := range expressions {
		ret = append(ret, ExprStmToLogicExpr(expr, input))
	}
	return ret
}
