package plan

import (
	"errors"
	"simpleDb/ast"
	"simpleDb/lexer"
	"strings"
)

func MakeLogicPlan(ast *ast.SelectStm, currentDB string) (LogicPlan, error) {
	scanLogicPlans, err := makeScanLogicPlans(ast.TableReferences, currentDB)
	if err != nil {
		return nil, err
	}
	joinLogicPlan := scanLogicPlans[0]
	if len(scanLogicPlans) >= 2 {
		joinLogicPlan = makeJoinLogicPlan(scanLogicPlans)
	}
	selectLogicPlan := makeSelectLogicPlan(joinLogicPlan, ast.Where)
	if ast.Groupby != nil {
		return MakeAggreLogicPlan(selectLogicPlan, ast)
	}
	orderByLogicPlan := makeOrderByLogicPlan(selectLogicPlan, ast.OrderBy, false)

	projectionsLogicPlan := makeProjectionLogicPlan(orderByLogicPlan, ast.SelectExpressions)
	limitLogicPlan := makeLimitLogicPlan(projectionsLogicPlan, ast.LimitStm)
	return limitLogicPlan, limitLogicPlan.TypeCheck()
}

func makeScanLogicPlans(tableRefs []ast.TableReferenceStm, currentDB string) (ret []LogicPlan, err error) {
	for _, tableRef := range tableRefs {
		switch tableRef.Tp {
		case ast.TableReferenceTableFactorTp:
			plan, err := makeScanLogicPlan(tableRef.TableReference.(ast.TableReferenceTableFactorStm), currentDB)
			if err != nil {
				return nil, err
			}
			ret = append(ret, plan)
		case ast.TableReferenceJoinTableTp: // Build scanLogicPlan for the join op.
			plan, err := makeScanLogicPlanForJoin(tableRef.TableReference.(ast.JoinedTableStm), currentDB)
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

func makeScanLogicPlan(tableRefTableFactorStm ast.TableReferenceTableFactorStm, currentDB string) (LogicPlan, error) {
	switch tableRefTableFactorStm.Tp {
	case ast.TableReferencePureTableNameTp:
		table := tableRefTableFactorStm.TableFactorReference.(ast.TableReferencePureTableRefStm)
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
	case ast.TableReferenceTableSubQueryTp, ast.TableReferenceSubTableReferenceStmTP:
		panic("doesn't support sub query currently")
	}
	return nil, nil
}

func splitSchemaAndTableName(schemaTable string) (schema, table string, err error) {
	splits := strings.Split(schemaTable, ".")
	if len(splits) >= 3 || len(splits[0]) == 0 || len(splits[1]) == 0 {
		err = errors.New("wrong table or schema format")
		return
	}
	return splits[0], splits[1], nil
}

func makeScanLogicPlanForJoin(joinTableStm ast.JoinedTableStm, currentDB string) (LogicPlan, error) {
	// a inorder traversal to build logic plan.
	leftLogicPlan, err := makeScanLogicPlan(joinTableStm.TableReference, currentDB)
	if err != nil {
		return nil, err
	}
	rightLogicPlan, err := buildLogicPlanForTableReferenceStm(joinTableStm.JoinedTableReference, currentDB)
	if err != nil {
		return nil, err
	}
	return &JoinLogicPlan{
		LeftLogicPlan:  leftLogicPlan,
		RightLogicPlan: rightLogicPlan,
		JoinType:       joinTableStm.JoinTp,
	}, nil
}

func buildLogicPlanForTableReferenceStm(tableRef ast.TableReferenceStm, currentDB string) (LogicPlan, error) {
	switch tableRef.Tp {
	case ast.TableReferenceTableFactorTp:
		return makeScanLogicPlan(tableRef.TableReference.(ast.TableReferenceTableFactorStm), currentDB)
	case ast.TableReferenceJoinTableTp:
		return makeScanLogicPlanForJoin(tableRef.TableReference.(ast.JoinedTableStm), currentDB)
	default:
		panic("wrong tableRef type")
	}
	return nil, nil
}

// len(tableRefs) >= 2
func makeJoinLogicPlan(input []LogicPlan) LogicPlan {
	leftLogicPlan := input[0]
	for i := 1; i < len(input); i++ {
		rightLogicPlan := input[i]
		leftLogicPlan = &JoinLogicPlan{
			LeftLogicPlan:  leftLogicPlan,
			RightLogicPlan: rightLogicPlan,
			JoinType:       ast.InnerJoin,
		}
	}
	return leftLogicPlan
}

func makeSelectLogicPlan(input LogicPlan, whereStm ast.WhereStm) *SelectionLogicPlan {
	return &SelectionLogicPlan{
		Input: input,
		Expr:  ExprStmToLogicExpr(whereStm),
	}
}

func ExprStmToLogicExpr(expr *ast.ExpressionStm) LogicExpr {
	var leftLogicExpr, rightLogicExpr LogicExpr
	_, isLeftExprExprStm := expr.LeftExpr.(*ast.ExpressionStm)
	if isLeftExprExprStm {
		leftLogicExpr = ExprStmToLogicExpr(expr.LeftExpr.(*ast.ExpressionStm))
	} else {
		leftLogicExpr = ExprTermStmToLogicExpr(expr.LeftExpr.(*ast.ExpressionTerm))
	}
	if expr.RightExpr == nil {
		return leftLogicExpr
	}
	_, isRightExprExprStm := expr.RightExpr.(*ast.ExpressionStm)
	if isRightExprExprStm {
		rightLogicExpr = ExprStmToLogicExpr(expr.RightExpr.(*ast.ExpressionStm))
	} else {
		rightLogicExpr = ExprTermStmToLogicExpr(expr.RightExpr.(*ast.ExpressionTerm))
	}
	return buildLogicExprWithOp(leftLogicExpr, rightLogicExpr, expr.Op)
}

func buildLogicExprWithOp(leftLogicExpr, rightLogicExpr LogicExpr, op ast.ExpressionOp) LogicExpr {
	switch op.Tp {
	case lexer.ADD:
		return AddLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.MINUS:
		return MinusLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.MUL:
		return MulLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.DIVIDE:
		return DivideLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.MOD:
		return ModLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.EQUAL:
		return EqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.IS:
		return IsLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.NOTEQUAL:
		return NotEqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.GREAT:
		return GreatLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.GREATEQUAL:
		return GreatEqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.LESS:
		return LessLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.LESSEQUAL:
		return LessEqualLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.AND:
		return AndLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
	case lexer.OR:
		return OrLogicExpr{Left: leftLogicExpr, Right: rightLogicExpr}
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

func ExprTermStmToLogicExpr(exprTerm *ast.ExpressionTerm) LogicExpr {
	var logicExpr LogicExpr
	switch exprTerm.Tp {
	case ast.LiteralExpressionTermTP:
		logicExpr = LiteralExprToLiteralLogicExpr(exprTerm.RealExprTerm.(ast.LiteralExpressionStm))
	case ast.IdentifierExpressionTermTP:
		logicExpr = IdentifierExprToIdentifierLogicExpr(exprTerm.RealExprTerm.(ast.IdentifierExpression))
	case ast.FuncCallExpressionTermTP:
		logicExpr = FuncCallExprToLogicExpr(exprTerm.RealExprTerm.(ast.FunctionCallExpressionStm))
	case ast.SubExpressionTermTP:
		logicExpr = SubExprTermToLogicExpr(exprTerm.RealExprTerm.(ast.SubExpressionTerm))
	default:
		panic("unknown expr term type")
	}
	if exprTerm.UnaryOp == ast.NegativeUnaryOpTp {
		return NegativeLogicExpr{Input: logicExpr}
	}
	return logicExpr
}

func LiteralExprToLiteralLogicExpr(literalExprStm ast.LiteralExpressionStm) LogicExpr {
	return LiteralLogicExpr{Data: literalExprStm}
}

func IdentifierExprToIdentifierLogicExpr(identifierExpr ast.IdentifierExpression) LogicExpr {
	return IdentifierLogicExpr{Ident: identifierExpr}
}

func FuncCallExprToLogicExpr(funcCallExpr ast.FunctionCallExpressionStm) LogicExpr {
	funcCallLogicExpr := FuncCallLogicExpr{FuncName: funcCallExpr.FuncName}
	for _, param := range funcCallExpr.Params {
		funcCallLogicExpr.Params = append(funcCallLogicExpr.Params, ExprStmToLogicExpr(param))
	}
	return funcCallLogicExpr
}

func SubExprTermToLogicExpr(subExpr ast.SubExpressionTerm) LogicExpr {
	expr := ast.ExpressionTerm(subExpr)
	return ExprTermStmToLogicExpr(&expr)
}

func OrderedExpressionToOrderedExprs(orderedExprs []*ast.OrderedExpressionStm) OrderedLogicExpr {
	ret := OrderedLogicExpr{}
	for _, expr := range orderedExprs {
		ret.expr = append(ret.expr, ExprStmToLogicExpr(expr.Expression))
		ret.asc = append(ret.asc, expr.Asc)
	}
	return ret
}

func makeOrderByLogicPlan(input LogicPlan, orderBy *ast.OrderByStm, isAggr bool) *OrderByLogicPlan {
	return &OrderByLogicPlan{
		Input:   input,
		OrderBy: OrderedExpressionToOrderedExprs(orderBy.Expressions),
		IsAggr:  isAggr,
	}
}

func makeLimitLogicPlan(input LogicPlan, limitStm *ast.LimitStm) *LimitLogicPlan {
	return &LimitLogicPlan{
		Input:  input,
		Count:  limitStm.Count,
		Offset: limitStm.Offset,
	}
}

func SelectExprToAsExprLogicExpr(selectExprs []*ast.SelectExpr) []AsLogicExpr {
	ret := make([]AsLogicExpr, len(selectExprs))
	for i := 0; i < len(selectExprs); i++ {
		as := AsLogicExpr{}
		as.Expr = ExprStmToLogicExpr(selectExprs[i].Expr)
		as.Alias = selectExprs[i].Alias
		ret[i] = as
	}
	return ret
}

func makeProjectionLogicPlan(input LogicPlan, selectExprStm *ast.SelectExpressionStm) *ProjectionLogicPlan {
	projectionLogicPlan := &ProjectionLogicPlan{
		Input: input,
	}
	switch selectExprStm.Tp {
	case ast.StarSelectExpressionTp:
		return projectionLogicPlan
	case ast.ExprSelectExpressionTp:
		projectionLogicPlan.Exprs = SelectExprToAsExprLogicExpr(selectExprStm.Expr.([]*ast.SelectExpr))
	}
	return projectionLogicPlan
}
