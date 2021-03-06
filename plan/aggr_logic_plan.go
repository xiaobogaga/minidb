package plan

import (
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/parser"
	"github.com/xiaobogaga/minidb/storage"
)

// For groupBy exprs.
// HashGroupBy.
type GroupByLogicPlan struct {
	Input       LogicPlan            `json:"group_by_input"`
	GroupByExpr []LogicExpr          `json:"group_by_expr"`
	AggrExprs   []AsLogicExpr        `json:"aggrs"`
	data        *storage.RecordBatch // All record batch from the input.
	keys        *storage.RecordBatch // The keys from groupBy clause
	retData     *storage.RecordBatch // The data will return by the AggrExprs
	index       int
}

func (groupBy *GroupByLogicPlan) Schema() *storage.TableSchema {
	ret := &storage.TableSchema{}
	for _, aggrExpr := range groupBy.AggrExprs {
		f := aggrExpr.toField()
		ret.Columns = append(ret.Columns, f)
	}
	return ret
}

func (groupBy *GroupByLogicPlan) String() string {
	return fmt.Sprintf("GroupByLogicPlan: %s groupBy %s", groupBy.Input, groupBy.GroupByExpr)
}

func (groupBy *GroupByLogicPlan) Child() []LogicPlan {
	return []LogicPlan{groupBy.Input}
}

func (groupBy *GroupByLogicPlan) MakeAggrExprs() {
	schema := groupBy.Input.Schema()
	var ret []AsLogicExpr
	for _, column := range schema.Columns {
		if column.Name == storage.DefaultRowKeyName {
			continue
		}
		name := column.ColumnName()
		ret = append(ret, AsLogicExpr{
			Expr: &IdentifierLogicExpr{
				Ident: []byte(name),
				input: groupBy.Input,
				Str:   name,
			},
		})
	}
	groupBy.AggrExprs = ret
}

func (groupBy *GroupByLogicPlan) TypeCheck() error {
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
	if len(groupBy.AggrExprs) == 0 {
		groupBy.MakeAggrExprs()
	}
	// Now we check whether aggrExprs is okay.
	for _, aggrExpr := range groupBy.AggrExprs {
		err := aggrExpr.TypeCheck()
		if err != nil {
			return err
		}
		err = aggrExpr.AggrTypeCheck(groupBy.GroupByExpr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (groupBy *GroupByLogicPlan) Execute() *storage.RecordBatch {
	if groupBy.data == nil {
		groupBy.InitializeData()
	}
	ret := groupBy.retData.Slice(groupBy.index, batchSize)
	groupBy.index += batchSize
	return ret
}

// GroupBy.
// |---|---|---|  group by col1, col2.
// |---|---|---|           |----|----|
func (groupBy *GroupByLogicPlan) InitializeData() {
	if groupBy.data != nil {
		return
	}
	// Load all data from input and calculate the data for the keys.
	groupBy.data = MakeEmptyRecordBatchFromSchema(groupBy.Input.Schema())
	groupBy.keys = &storage.RecordBatch{
		Fields:  make([]storage.Field, len(groupBy.GroupByExpr)),
		Records: make([]*storage.ColumnVector, len(groupBy.GroupByExpr)),
	}
	for i, groupByExpr := range groupBy.GroupByExpr {
		f := groupByExpr.toField()
		groupBy.keys.Fields[i] = f
		groupBy.keys.Records[i] = &storage.ColumnVector{Field: f}
	}
	for {
		batch := groupBy.Input.Execute()
		if batch == nil {
			break
		}
		groupBy.data.Append(batch)
		// Now we calculate the values of keys.
		for j, groupByExpr := range groupBy.GroupByExpr {
			groupBy.keys.Records[j].Appends(groupByExpr.Evaluate(batch))
		}
	}

	// Here we have all data including keys.
	// Table1(data), Table2(keys) - group by keys.
	// |-|-|-|     , |-|-|-|

	// Now builds accumulators.
	groupBy.retData = MakeEmptyRecordBatchFromSchema(groupBy.Schema())
	keyMap := map[string][]LogicExpr{}
	var keys []string // To preserved the data order.
	for i := 0; i < groupBy.keys.RowCount(); i++ {
		key := string(groupBy.keys.RowKey(i))
		value, ok := keyMap[key]
		if !ok {
			keys = append(keys, key)
			value = groupBy.CloneAggrExpr(false)
			keyMap[key] = value
		}
		for _, expr := range value {
			// Accumulate row i at groupBy.data.
			expr.Accumulate(i, groupBy.data)
		}
	}
	// Now we have accumulate all data. It's time to collect all individual group now.
	for _, key := range keys {
		values := keyMap[key]
		for i, value := range values {
			groupBy.retData.Records[i].Append(value.AccumulateValue())
		}
	}
}

func (groupBy *GroupByLogicPlan) CloneAggrExpr(needAccumulator bool) (ret []LogicExpr) {
	ret = make([]LogicExpr, len(groupBy.AggrExprs))
	for i, aggrExpr := range groupBy.AggrExprs {
		ret[i] = aggrExpr.Clone(needAccumulator)
	}
	return ret
}

func (groupBy *GroupByLogicPlan) Reset() {
	groupBy.data = nil
	groupBy.keys = nil
	groupBy.retData = nil
	groupBy.index = 0
}

// For Having condition
type HavingLogicPlan struct {
	Input *GroupByLogicPlan `json:"having_input"`
	Expr  LogicExpr         `json:"Expr"`
}

func (having *HavingLogicPlan) Schema() *storage.TableSchema {
	// Should be the same schema as Expr.
	return having.Input.Schema()
}

func (having *HavingLogicPlan) String() string {
	return fmt.Sprintf("HavingLogicPlan: %s having %s", having.Input, having.Expr)
}

func (having *HavingLogicPlan) Child() []LogicPlan {
	return []LogicPlan{having.Input}
}

func (having *HavingLogicPlan) TypeCheck() error {
	err := having.Input.TypeCheck()
	if err != nil {
		return err
	}
	return having.Expr.AggrTypeCheck(having.Input.GroupByExpr)
}

func (having *HavingLogicPlan) Execute() (ret *storage.RecordBatch) {
	i := 0
	for i < batchSize {
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
	}
	return
}

func (having *HavingLogicPlan) Reset() {
	having.Input.Reset()
}

func MakeAggreLogicPlan(input LogicPlan, ast *parser.SelectStm) (LogicPlan, error) {
	groupByLogicPlan := makeGroupByLogicPlan(input, ast.Groupby, ast.SelectExpressions)
	// Having similar to projections for aggregation, the Expr must be either included in the group by Expr.
	// or must be an aggregation function.
	havingLogicPlan := makeHavingLogicPlan(groupByLogicPlan, ast.Having)
	// Order by similar to projections for aggregation, the Expr must be either included in the group by Expr,
	// or must be an aggregation function.
	orderByLogicPlan := makeOrderByLogicPlan(havingLogicPlan, ast.OrderBy, true)
	limitLogicPlan := makeLimitLogicPlan(orderByLogicPlan, ast.LimitStm)
	return limitLogicPlan, limitLogicPlan.TypeCheck()
}

func makeHavingLogicPlan(input *GroupByLogicPlan, having parser.HavingStm) LogicPlan {
	if having == nil {
		return input
	}
	return &HavingLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(having, input),
	}
}

func makeGroupByLogicPlan(input LogicPlan, groupBy *parser.GroupByStm, selectExprStm *parser.SelectExpressionStm) *GroupByLogicPlan {
	ret := &GroupByLogicPlan{
		Input:       input,
		GroupByExpr: ExprStmsToLogicExprs(*groupBy, input),
	}
	switch selectExprStm.Tp {
	case parser.StarSelectExpressionTp:
	case parser.ExprSelectExpressionTp:
		ret.AggrExprs = SelectExprToAsExprLogicExpr(selectExprStm.Expr.([]*parser.SelectExpr), input)
	}
	return ret
}

func ExprStmsToLogicExprs(expressions []*parser.ExpressionStm, input LogicPlan) (ret []LogicExpr) {
	for _, expr := range expressions {
		ret = append(ret, ExprStmToLogicExpr(expr, input))
	}
	return ret
}
