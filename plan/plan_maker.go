package plan

import (
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/parser"
	"strings"
)

func MakePlan(ast *parser.SelectStm, currentDB string) (Plan, error) {
	scanPlans, err := makeScanPlans(ast.TableReferences, currentDB)
	if err != nil {
		return nil, err
	}
	Plan := makeJoinPlan(scanPlans)
	selectPlan := makeSelectPlan(Plan, ast.Where)
	if ast.Groupby != nil {
		return MakeAggrePlan(selectPlan, ast)
	}
	// having is the same as where when no group by.
	if ast.Having != nil {
		selectPlan = makeSelectPlan(selectPlan, parser.WhereStm(ast.Having))
	}
	orderByPlan := makeOrderByPlan(selectPlan, ast.OrderBy, false)
	projectionsPlan := makeProjectionPlan(orderByPlan, ast.SelectExpressions)
	limitPlan := makeLimitPlan(projectionsPlan, ast.LimitStm)
	return limitPlan, limitPlan.TypeCheck()
}

func makeScanPlans(tableRefs []parser.TableReferenceStm, currentDB string) (ret []Plan, err error) {
	for _, tableRef := range tableRefs {
		switch tableRef.Tp {
		case parser.TableReferenceTableFactorTp:
			plan, err := makeScanPlan(tableRef.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
			if err != nil {
				return nil, err
			}
			ret = append(ret, plan)
		case parser.TableReferenceJoinTableTp: // Build scanPlan for the join op.
			plan, err := makeScanPlanForJoin(tableRef.TableReference.(parser.JoinedTableStm), currentDB)
			if err != nil {
				return nil, err
			}
			ret = append(ret, plan)
		default:
			panic("unsupported table ref type")
		}
	}
	return
}

func makeScanPlan(tableRefTableFactorStm parser.TableReferenceTableFactorStm, currentDB string) (Plan, error) {
	switch tableRefTableFactorStm.Tp {
	case parser.TableReferencePureTableNameTp:
		table := tableRefTableFactorStm.TableFactorReference.(parser.TableReferencePureTableRefStm)
		schemaName, tableName, err := splitSchemaAndTableName(table.TableName)
		if err != nil {
			return nil, err
		}
		if schemaName == "" {
			schemaName = currentDB
		}
		if table.Alias == "" {
			table.Alias = table.TableName
		}
		return &ScanPlan{
			Name:       tableName,
			SchemaName: schemaName,
			Alias:      table.Alias,
			Input:      &TableScan{Name: tableName, SchemaName: schemaName},
		}, nil
	case parser.TableReferenceTableSubQueryTp, parser.TableReferenceSubTableReferenceStmTP:
		panic("doesn't support sub query currently")
	}
	return nil, nil
}

func splitSchemaAndTableName(schemaTable string) (schema, table string, err error) {
	splits := strings.Split(schemaTable, ".")
	if len(splits) >= 3 {
		err = errors.New("wrong table or schema format")
		return
	}
	switch len(splits) {
	case 1:
		table = splits[0]
	case 2:
		schema = splits[0]
		table = splits[1]
	}
	return
}

func joinSpecToExpr(joinSpex *parser.JoinSpecification, input *JoinPlan) (expr Expr) {
	if joinSpex == nil {
		return nil
	}
	switch joinSpex.Tp {
	case parser.JoinSpecificationON:
		expr = ExprStmToExpr(joinSpex.Condition.(*parser.ExpressionStm), input)
	case parser.JoinSpecificationUsing:
		return buildExprForUsing(joinSpex, input)
	default:
		panic("unknown join tp")
	}
	return
}

func buildExprForUsing(joinSpex *parser.JoinSpecification, input *JoinPlan) (expr Expr) {
	cols := joinSpex.Condition.([]string)
	for i, col := range cols {
		leftColName := []byte(fmt.Sprintf("%s.%s", input.LeftPlan.Schema().TableName(), col))
		rightColName := []byte(fmt.Sprintf("%s.%s", input.RightPlan.Schema().TableName(), col))
		if i == 0 {
			expr = EqualExpr{
				Left:  &IdentifierExpr{Ident: leftColName, input: input},
				Right: &IdentifierExpr{Ident: rightColName, input: input},
				Name:  "equal",
			}
			continue
		}
		expr = AndExpr{
			Left: expr,
			Right: EqualExpr{
				Left:  &IdentifierExpr{Ident: leftColName, input: input},
				Right: &IdentifierExpr{Ident: rightColName, input: input},
				Name:  "equal",
			},
			Name: "and",
		}
	}
	return
}

// Build join plan recursively.
func makeScanPlanForJoin(joinTableStm parser.JoinedTableStm, currentDB string) (Plan, error) {
	// a inorder traversal to build  plan.
	leftPlan, err := makeScanPlan(joinTableStm.TableFactor, currentDB)
	if err != nil {
		return nil, err
	}
	rightPlan, err := makeScanPlan(joinTableStm.JoinFactors[0].JoinedTableReference.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
	if err != nil {
		return nil, err
	}
	joinPlan := NewJoinPlan(leftPlan, rightPlan, joinTableStm.JoinFactors[0].JoinTp)
	expr := joinSpecToExpr(joinTableStm.JoinFactors[0].JoinSpec, joinPlan)
	if expr == nil {
		return buildRemainJoinPlan(joinPlan, joinTableStm.JoinFactors[1:], currentDB)
	}
	plan := &SelectionPlan{Input: joinPlan, Expr: expr}
	return buildRemainJoinPlan(plan, joinTableStm.JoinFactors[1:], currentDB)
}

// Build  plan for tableFactors[1:]
func buildRemainJoinPlan(selectionPlan Plan, tableFactors []parser.JoinFactor, currentDB string) (Plan, error) {
	if len(tableFactors) == 0 {
		return selectionPlan, nil
	}
	rightPlan, err := makeScanPlan(tableFactors[0].JoinedTableReference.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
	if err != nil {
		return nil, err
	}
	joinPlan := NewJoinPlan(selectionPlan, rightPlan, tableFactors[0].JoinTp)
	expr := joinSpecToExpr(tableFactors[0].JoinSpec, joinPlan)
	if expr == nil {
		return buildRemainJoinPlan(joinPlan, tableFactors[1:], currentDB)
	}
	plan := &SelectionPlan{Input: joinPlan, Expr: expr}
	return buildRemainJoinPlan(plan, tableFactors[1:], currentDB)
}

func buildPlanForTableReferenceStm(tableRef parser.TableReferenceStm, currentDB string) (Plan, error) {
	switch tableRef.Tp {
	case parser.TableReferenceTableFactorTp:
		return makeScanPlan(tableRef.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
	case parser.TableReferenceJoinTableTp:
		return makeScanPlanForJoin(tableRef.TableReference.(parser.JoinedTableStm), currentDB)
	default:
		panic("wrong tableRef type")
	}
	return nil, nil
}

// len(tableRefs) >= 2
func makeJoinPlan(input []Plan) Plan {
	if len(input) <= 1 {
		return input[0]
	}
	leftPlan := input[0]
	for i := 1; i < len(input); i++ {
		leftPlan = NewJoinPlan(leftPlan, input[i], parser.InnerJoin)
	}
	return leftPlan
}

func makeSelectPlan(input Plan, whereStm parser.WhereStm) Plan {
	if whereStm == nil {
		return input
	}
	return &SelectionPlan{
		Input: input,
		Expr:  ExprStmToExpr(whereStm, input),
	}
}

func ExprStmToExpr(expr *parser.ExpressionStm, input Plan) Expr {
	if expr == nil {
		return nil
	}
	var leftExpr, rightExpr Expr
	_, isLeftExprExprStm := expr.LeftExpr.(*parser.ExpressionStm)
	if isLeftExprExprStm {
		leftExpr = ExprStmToExpr(expr.LeftExpr.(*parser.ExpressionStm), input)
	} else {
		leftExpr = ExprTermStmToExpr(expr.LeftExpr.(*parser.ExpressionTerm), input)
	}
	if expr.RightExpr == nil {
		return leftExpr
	}
	_, isRightExprExprStm := expr.RightExpr.(*parser.ExpressionStm)
	if isRightExprExprStm {
		rightExpr = ExprStmToExpr(expr.RightExpr.(*parser.ExpressionStm), input)
	} else {
		rightExpr = ExprTermStmToExpr(expr.RightExpr.(*parser.ExpressionTerm), input)
	}
	return buildExprWithOp(leftExpr, rightExpr, expr.Op)
}

func buildExprWithOp(leftExpr, rightExpr Expr, op *parser.ExpressionOp) Expr {
	switch op.Tp {
	case parser.ADD:
		return AddExpr{Left: leftExpr, Right: rightExpr, Name: "+"}
	case parser.MINUS:
		return MinusExpr{Left: leftExpr, Right: rightExpr, Name: "-"}
	case parser.MUL:
		return MulExpr{Left: leftExpr, Right: rightExpr, Name: "*"}
	case parser.DIVIDE:
		return DivideExpr{Left: leftExpr, Right: rightExpr, Name: "/"}
	case parser.MOD:
		return ModExpr{Left: leftExpr, Right: rightExpr, Name: "%"}
	case parser.EQUAL:
		return EqualExpr{Left: leftExpr, Right: rightExpr, Name: "="}
	case parser.IS:
		return IsExpr{Left: leftExpr, Right: rightExpr, Name: "is"}
	case parser.NOTEQUAL:
		return NotEqualExpr{Left: leftExpr, Right: rightExpr, Name: "!="}
	case parser.GREAT:
		return GreatExpr{Left: leftExpr, Right: rightExpr, Name: ">"}
	case parser.GREATEQUAL:
		return GreatEqualExpr{Left: leftExpr, Right: rightExpr, Name: ">="}
	case parser.LESS:
		return LessExpr{Left: leftExpr, Right: rightExpr, Name: "<"}
	case parser.LESSEQUAL:
		return LessEqualExpr{Left: leftExpr, Right: rightExpr, Name: "<="}
	case parser.AND:
		return AndExpr{Left: leftExpr, Right: rightExpr, Name: "and"}
	case parser.OR:
		return OrExpr{Left: leftExpr, Right: rightExpr, Name: "or"}
		// case lexer.DOT:
		// For DotExpr, the leftExpr must be a IdentifierAggrExpr and rightExpt must be
		// a DotExpr or IdentifierAggrExpr.
		// A little tricky
		// dotExpr := DotExpr{Left: leftExpr, Right: rightExpr}
		// dotExpr.ReBuildIdentifierType()
		// return dotExpr
	default:
		panic("wrong op type")
	}
}

func ExprTermStmToExpr(exprTerm *parser.ExpressionTerm, input Plan) Expr {
	var Expr Expr
	switch exprTerm.Tp {
	case parser.LiteralExpressionTermTP:
		Expr = LiteralExprToLiteralExpr(exprTerm.RealExprTerm.(parser.LiteralExpressionStm))
	case parser.IdentifierExpressionTermTP:
		Expr = IdentifierExprToIdentifierExpr(exprTerm.RealExprTerm.(parser.IdentifierExpression), input)
	case parser.FuncCallExpressionTermTP:
		Expr = FuncCallExprToExpr(exprTerm.RealExprTerm.(parser.FunctionCallExpressionStm), input)
	case parser.SubExpressionTermTP:
		Expr = ExprStmToExpr(exprTerm.RealExprTerm.(*parser.ExpressionStm), input)
	default:
		panic("unknown Expr term type")
	}
	if exprTerm.UnaryOp == parser.NegativeUnaryOpTp {
		return NegativeExpr{Expr: Expr}
	}
	return Expr
}

func LiteralExprToLiteralExpr(literalExprStm parser.LiteralExpressionStm) Expr {
	ret := LiteralExpr{Data: literalExprStm}
	ret.Str = ret.String()
	return ret
}

func IdentifierExprToIdentifierExpr(identifierExpr parser.IdentifierExpression, input Plan) Expr {
	return &IdentifierExpr{Ident: identifierExpr, input: input, Str: string(identifierExpr)}
}

func FuncCallExprToExpr(funcCallExpr parser.FunctionCallExpressionStm, input Plan) Expr {
	params := make([]Expr, len(funcCallExpr.Params))
	for i, param := range funcCallExpr.Params {
		params[i] = ExprStmToExpr(param, input)
	}
	ret := MakeFuncCallExpr(funcCallExpr.FuncName, params)
	return ret
}

//func SubExprTermToExpr(subExpr parser.SubExpressionTerm, input Plan) Expr {
//	Expr := parser.ExpressionTerm(subExpr)
//	return ExprTermStmToExpr(&Expr, input)
//}

func OrderedExpressionToOrderedExprs(orderedExprs []*parser.OrderedExpressionStm, input Plan) OrderByExpr {
	ret := OrderByExpr{}
	for _, expr := range orderedExprs {
		ret.Expr = append(ret.Expr, ExprStmToExpr(expr.Expression, input))
		ret.Asc = append(ret.Asc, expr.Asc)
	}
	return ret
}

func makeOrderByPlan(input Plan, orderBy *parser.OrderByStm, isAggr bool) Plan {
	if orderBy == nil {
		return input
	}
	return &OrderByPlan{
		Input:   input,
		OrderBy: OrderedExpressionToOrderedExprs(orderBy.Expressions, input),
		IsAggr:  isAggr,
	}
}

func makeLimitPlan(input Plan, limitStm *parser.LimitStm) Plan {
	if limitStm == nil {
		return input
	}
	return &LimitPlan{
		Input:  input,
		Count:  limitStm.Count,
		Offset: limitStm.Offset,
	}
}

func SelectExprToAsExpr(selectExprs []*parser.SelectExpr, input Plan) []AsExpr {
	ret := make([]AsExpr, len(selectExprs))
	for i := 0; i < len(selectExprs); i++ {
		as := AsExpr{}
		as.Expr = ExprStmToExpr(selectExprs[i].Expr, input)
		as.Alias = selectExprs[i].Alias
		ret[i] = as
	}
	return ret
}

func makeProjectionPlan(input Plan, selectExprStm *parser.SelectExpressionStm) *ProjectionPlan {
	projectionPlan := &ProjectionPlan{
		Input: input,
	}
	switch selectExprStm.Tp {
	case parser.StarSelectExpressionTp:
		return projectionPlan
	case parser.ExprSelectExpressionTp:
		projectionPlan.Exprs = SelectExprToAsExpr(selectExprStm.Expr.([]*parser.SelectExpr), input)
	}
	return projectionPlan
}
