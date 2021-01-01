package plan

import (
	"errors"
	"fmt"
	"minidb/parser"
	"strings"
)

func MakeLogicPlan(ast *parser.SelectStm, currentDB string) (LogicPlan, error) {
	scanLogicPlans, err := makeScanLogicPlans(ast.TableReferences, currentDB)
	if err != nil {
		return nil, err
	}
	logicPlan := makeJoinLogicPlan(scanLogicPlans)
	selectLogicPlan := makeSelectLogicPlan(logicPlan, ast.Where)
	if ast.Groupby != nil {
		return MakeAggreLogicPlan(selectLogicPlan, ast)
	}
	orderByLogicPlan := makeOrderByLogicPlan(selectLogicPlan, ast.OrderBy, false)
	projectionsLogicPlan := makeProjectionLogicPlan(orderByLogicPlan, ast.SelectExpressions)
	limitLogicPlan := makeLimitLogicPlan(projectionsLogicPlan, ast.LimitStm)
	return limitLogicPlan, limitLogicPlan.TypeCheck()
}

func makeScanLogicPlans(tableRefs []parser.TableReferenceStm, currentDB string) (ret []LogicPlan, err error) {
	for _, tableRef := range tableRefs {
		switch tableRef.Tp {
		case parser.TableReferenceTableFactorTp:
			plan, err := makeScanLogicPlan(tableRef.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
			if err != nil {
				return nil, err
			}
			ret = append(ret, plan)
		case parser.TableReferenceJoinTableTp: // Build scanLogicPlan for the join op.
			plan, err := makeScanLogicPlanForJoin(tableRef.TableReference.(parser.JoinedTableStm), currentDB)
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

func makeScanLogicPlan(tableRefTableFactorStm parser.TableReferenceTableFactorStm, currentDB string) (LogicPlan, error) {
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
		return &ScanLogicPlan{
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

func joinSpecToExpr(joinSpex *parser.JoinSpecification, input *JoinLogicPlan) (expr LogicExpr) {
	if joinSpex == nil {
		return nil
	}
	switch joinSpex.Tp {
	case parser.JoinSpecificationON:
		expr = ExprStmToLogicExpr(joinSpex.Condition.(*parser.ExpressionStm), input)
	case parser.JoinSpecificationUsing:
		return buildLogicExprForUsing(joinSpex, input)
	default:
		panic("unknown join tp")
	}
	return
}

func buildLogicExprForUsing(joinSpex *parser.JoinSpecification, input *JoinLogicPlan) (expr LogicExpr) {
	cols := joinSpex.Condition.([]string)
	for i, col := range cols {
		leftColName := []byte(fmt.Sprintf("%s.%s", input.LeftLogicPlan.Schema().TableName(), col))
		rightColName := []byte(fmt.Sprintf("%s.%s", input.RightLogicPlan.Schema().TableName(), col))
		if i == 0 {
			expr = EqualLogicExpr{
				Left:  IdentifierLogicExpr{Ident: leftColName, input: input},
				Right: IdentifierLogicExpr{Ident: rightColName, input: input},
				Name:  "equal",
			}
			continue
		}
		expr = AndLogicExpr{
			Left: expr,
			Right: EqualLogicExpr{
				Left:  IdentifierLogicExpr{Ident: leftColName, input: input},
				Right: IdentifierLogicExpr{Ident: rightColName, input: input},
				Name:  "equal",
			},
			Name: "and",
		}
	}
	return
}

func makeScanLogicPlanForJoin(joinTableStm parser.JoinedTableStm, currentDB string) (LogicPlan, error) {
	// a inorder traversal to build logic plan.
	leftLogicPlan, err := makeScanLogicPlan(joinTableStm.TableReference, currentDB)
	if err != nil {
		return nil, err
	}
	rightLogicPlan, err := buildLogicPlanForTableReferenceStm(joinTableStm.JoinedTableReference, currentDB)
	if err != nil {
		return nil, err
	}
	joinPlan := NewJoinLogicPlan(leftLogicPlan, rightLogicPlan, joinTableStm.JoinTp)
	if joinTableStm.JoinSpec == nil {
		return joinPlan, nil
	}
	expr := joinSpecToExpr(joinTableStm.JoinSpec, joinPlan)
	return &SelectionLogicPlan{Input: joinPlan, Expr: expr}, nil
}

func buildLogicPlanForTableReferenceStm(tableRef parser.TableReferenceStm, currentDB string) (LogicPlan, error) {
	switch tableRef.Tp {
	case parser.TableReferenceTableFactorTp:
		return makeScanLogicPlan(tableRef.TableReference.(parser.TableReferenceTableFactorStm), currentDB)
	case parser.TableReferenceJoinTableTp:
		return makeScanLogicPlanForJoin(tableRef.TableReference.(parser.JoinedTableStm), currentDB)
	default:
		panic("wrong tableRef type")
	}
	return nil, nil
}

// len(tableRefs) >= 2
func makeJoinLogicPlan(input []LogicPlan) LogicPlan {
	if len(input) <= 1 {
		return input[0]
	}
	leftLogicPlan := input[0]
	for i := 1; i < len(input); i++ {
		leftLogicPlan = NewJoinLogicPlan(leftLogicPlan, input[i], parser.InnerJoin)
	}
	return leftLogicPlan
}

func makeSelectLogicPlan(input LogicPlan, whereStm parser.WhereStm) LogicPlan {
	if whereStm == nil {
		return input
	}
	return &SelectionLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(whereStm, input),
	}
}

func ExprStmToLogicExpr(expr *parser.ExpressionStm, input LogicPlan) LogicExpr {
	if expr == nil {
		return nil
	}
	var leftLogicExpr, rightLogicExpr LogicExpr
	_, isLeftExprExprStm := expr.LeftExpr.(*parser.ExpressionStm)
	if isLeftExprExprStm {
		leftLogicExpr = ExprStmToLogicExpr(expr.LeftExpr.(*parser.ExpressionStm), input)
	} else {
		leftLogicExpr = ExprTermStmToLogicExpr(expr.LeftExpr.(*parser.ExpressionTerm), input)
	}
	if expr.RightExpr == nil {
		return leftLogicExpr
	}
	_, isRightExprExprStm := expr.RightExpr.(*parser.ExpressionStm)
	if isRightExprExprStm {
		rightLogicExpr = ExprStmToLogicExpr(expr.RightExpr.(*parser.ExpressionStm), input)
	} else {
		rightLogicExpr = ExprTermStmToLogicExpr(expr.RightExpr.(*parser.ExpressionTerm), input)
	}
	return buildLogicExprWithOp(leftLogicExpr, rightLogicExpr, expr.Op)
}

func buildLogicExprWithOp(leftLogicExpr, rightLogicExpr LogicExpr, op *parser.ExpressionOp) LogicExpr {
	switch op.Tp {
	case parser.ADD:
		return AddLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "+"}
	case parser.MINUS:
		return MinusLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "-"}
	case parser.MUL:
		return MulLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "*"}
	case parser.DIVIDE:
		return DivideLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "/"}
	case parser.MOD:
		return ModLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "%"}
	case parser.EQUAL:
		return EqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "="}
	case parser.IS:
		return IsLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "is"}
	case parser.NOTEQUAL:
		return NotEqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "!="}
	case parser.GREAT:
		return GreatLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: ">"}
	case parser.GREATEQUAL:
		return GreatEqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: ">="}
	case parser.LESS:
		return LessLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "<"}
	case parser.LESSEQUAL:
		return LessEqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "<="}
	case parser.AND:
		return AndLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "and"}
	case parser.OR:
		return OrLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr, Name: "or"}
		// case lexer.DOT:
		// For DotLogicExpr, the leftLogicExpr must be a IdentifierLogicAggrExpr and rightLogicExpt must be
		// a DotLogicExpr or IdentifierLogicAggrExpr.
		// A little tricky
		// dotLogicExpr := DotLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
		// dotLogicExpr.ReBuildIdentifierType()
		// return dotLogicExpr
	default:
		panic("wrong op type")
	}
}

func ExprTermStmToLogicExpr(exprTerm *parser.ExpressionTerm, input LogicPlan) LogicExpr {
	var logicExpr LogicExpr
	switch exprTerm.Tp {
	case parser.LiteralExpressionTermTP:
		logicExpr = LiteralExprToLiteralLogicExpr(exprTerm.RealExprTerm.(parser.LiteralExpressionStm))
	case parser.IdentifierExpressionTermTP:
		logicExpr = IdentifierExprToIdentifierLogicExpr(exprTerm.RealExprTerm.(parser.IdentifierExpression), input)
	case parser.FuncCallExpressionTermTP:
		logicExpr = FuncCallExprToLogicExpr(exprTerm.RealExprTerm.(parser.FunctionCallExpressionStm), input)
	case parser.SubExpressionTermTP:
		logicExpr = ExprStmToLogicExpr(exprTerm.RealExprTerm.(*parser.ExpressionStm), input)
	default:
		panic("unknown Expr term type")
	}
	if exprTerm.UnaryOp == parser.NegativeUnaryOpTp {
		return NegativeLogicExpr{Expr: logicExpr}
	}
	return logicExpr
}

func LiteralExprToLiteralLogicExpr(literalExprStm parser.LiteralExpressionStm) LogicExpr {
	ret := LiteralLogicExpr{Data: literalExprStm}
	ret.Str = ret.String()
	return ret
}

func IdentifierExprToIdentifierLogicExpr(identifierExpr parser.IdentifierExpression, input LogicPlan) LogicExpr {
	return IdentifierLogicExpr{Ident: identifierExpr, input: input, Str: string(identifierExpr)}
}

func FuncCallExprToLogicExpr(funcCallExpr parser.FunctionCallExpressionStm, input LogicPlan) LogicExpr {
	params := make([]LogicExpr, len(funcCallExpr.Params))
	for i, param := range funcCallExpr.Params {
		params[i] = ExprStmToLogicExpr(param, input)
	}
	funcCallLogicExpr := MakeFuncCallLogicExpr(funcCallExpr.FuncName, params)
	return funcCallLogicExpr
}

//func SubExprTermToLogicExpr(subExpr parser.SubExpressionTerm, input LogicPlan) LogicExpr {
//	Expr := parser.ExpressionTerm(subExpr)
//	return ExprTermStmToLogicExpr(&Expr, input)
//}

func OrderedExpressionToOrderedExprs(orderedExprs []*parser.OrderedExpressionStm, input LogicPlan) OrderByLogicExpr {
	ret := OrderByLogicExpr{}
	for _, expr := range orderedExprs {
		ret.Expr = append(ret.Expr, ExprStmToLogicExpr(expr.Expression, input))
		ret.Asc = append(ret.Asc, expr.Asc)
	}
	return ret
}

func makeOrderByLogicPlan(input LogicPlan, orderBy *parser.OrderByStm, isAggr bool) LogicPlan {
	if orderBy == nil {
		return input
	}
	return &OrderByLogicPlan{
		Input:   input,
		OrderBy: OrderedExpressionToOrderedExprs(orderBy.Expressions, input),
		IsAggr:  isAggr,
	}
}

func makeLimitLogicPlan(input LogicPlan, limitStm *parser.LimitStm) LogicPlan {
	if limitStm == nil {
		return input
	}
	return &LimitLogicPlan{
		Input:  input,
		Count:  limitStm.Count,
		Offset: limitStm.Offset,
	}
}

func SelectExprToAsExprLogicExpr(selectExprs []*parser.SelectExpr, input LogicPlan) []AsLogicExpr {
	ret := make([]AsLogicExpr, len(selectExprs))
	for i := 0; i < len(selectExprs); i++ {
		as := AsLogicExpr{}
		as.Expr = ExprStmToLogicExpr(selectExprs[i].Expr, input)
		as.Alias = selectExprs[i].Alias
		ret[i] = as
	}
	return ret
}

func makeProjectionLogicPlan(input LogicPlan, selectExprStm *parser.SelectExpressionStm) *ProjectionLogicPlan {
	projectionLogicPlan := &ProjectionLogicPlan{
		Input: input,
	}
	switch selectExprStm.Tp {
	case parser.StarSelectExpressionTp:
		return projectionLogicPlan
	case parser.ExprSelectExpressionTp:
		projectionLogicPlan.Exprs = SelectExprToAsExprLogicExpr(selectExprStm.Expr.([]*parser.SelectExpr), input)
	}
	return projectionLogicPlan
}
