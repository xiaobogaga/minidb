package plan

// It turns out aggregation expr is quite different.
// So put all aggregate expression implementation here.

type AggrExpr interface {
	LogicExpr
}

// select user_id, shop_id, sum(item_size) from user_shop where user_id > 0 group by user_id, shop_id
// The plan looks like:
//		TableScan (user_shop)
//       	    |
//   Select (where user_id > 0)
//              |
//   GroupBy (user_id, shop_id)
// select logic plan returns RecordBatch (by the Evaluate function)
// Our implementation for the GroupByPlan can return a recordBatch by adding a new column, aka the group column.
// For example, the table layout after GroupBy can be:
// ++++++|++++++++|++++++++|+++++++++++
// |group| user_id| shop_id| item_size|
// |    0|       1|       2|         2|
// |    0|       1|       2|         3|
// |    1|       2|       1|         0|
// |    2|       2|       2|         2|
// |    1|       2|       2|         4|
// ++++++|++++++++|++++++++|++++++++++|
// However, for normal(non aggregate) expression, the IdentLogicExpr will output all records in it's input recordBatch.
// and they won't handle the groupColumn.
// So we need to refine a new IdentAggreExpr to handle the group column.

type IdentifierAggrLogicExpr struct {
}

func (identAggr IdentifierAggrLogicExpr) Evaluate() {

}
