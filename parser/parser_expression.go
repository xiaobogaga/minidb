package parser

// For expression, compared to mysql, we use a simplified version and only a subset expressions of mysql
// are supported. An expression statement is like:
// term (ope term)
// a term can be:
// * literal | (expr) | identifier | functionCall |
// where functionCall is like:
// funcName(expr,...)
// where ope supports:
// +, -, *, /, %, =, IS, !=, IS NOT, >, >=, <, <=, AND, OR,
// Note: currently we don't consider [NOT] IN, [NOT] LIKE
// Note: literal can be -5
func (parser *Parser) resolveExpression() (expr *ExpressionStm, err error) {
	exprTerm, err := parser.parseExpressionTerm()
	if err != nil {
		return nil, err
	}
	var exprs []*ExpressionTerm
	exprs = append(exprs, exprTerm)
	var ops []*ExpressionOp
	for {
		token, ok := parser.NextToken()
		if !ok || !isTokenAOpe(token) {
			parser.UnReadToken()
			break
		}
		rightExprTerm, err := parser.parseExpressionTerm()
		if err != nil {
			return nil, err
		}
		ops = append(ops, parser.LexerOpToExpressionOp(token.Tp))
		exprs = append(exprs, rightExprTerm)
	}
	return parser.buildExpressionsTree(ops, exprs), nil
}

func (parser *Parser) LexerOpToExpressionOp(op TokenType) *ExpressionOp {
	switch op {
	case PLUS:
		return OperationAdd
	case MINUS:
		return OperationMinus
	case MUL:
		return OperationMul
	case DIVIDE:
		return OperationDivide
	case MOD:
		return OperationMod
	case EQUAL:
		return OperationEqual
	case IS:
		return OperationIs
	case NOTEQUAL:
		return OperationNotEqual
	case GREAT:
		return OperationGreat
	case GREATEQUAL:
		return OperationGreatEqual
	case LESS:
		return OperationLess
	case LESSEQUAL:
		return OperationLessEqual
	case AND:
		return OperationAnd
	case OR:
		return OperationOr
	// case lexer.OR + 1:
	//	return ast.OperationISNot
	// case lexer.DOT:
	//  return ast.OperationDot
	default:
		panic("unknown op type")
	}
}

func (parser *Parser) buildExpressionsTree(ops []*ExpressionOp, exprTerms []*ExpressionTerm) *ExpressionStm {
	if len(ops) == 0 {
		return &ExpressionStm{LeftExpr: exprTerms[0]}
	}
	if len(ops) == 1 {
		return &ExpressionStm{LeftExpr: exprTerms[0], Op: ops[0], RightExpr: exprTerms[1]}
	}
	expressionStack := make([]interface{}, 0, len(exprTerms))
	for _, exprTerm := range exprTerms {
		expressionStack = append(expressionStack, exprTerm)
	}
	ret, _ := parser.buildExpressionsTree0(ops, expressionStack, 0, 0)
	return ret.(*ExpressionStm)
}

func (parser *Parser) buildExpressionsTree0(ops []*ExpressionOp, exprTerms []interface{}, loc int, minPriority int) (interface{}, int) {
	lhs := exprTerms[loc]
	i := loc
	for i < len(ops) && ops[i].Priority >= minPriority {
		op := ops[i]
		rhs := exprTerms[i+1]
		j := i + 1
		for j < len(ops) && ops[j].Priority > op.Priority {
			rhs, j = parser.buildExpressionsTree0(ops, exprTerms, j, ops[j].Priority)
		}
		lhs = parser.makeNewExpression(lhs, rhs, op)
		exprTerms[j] = lhs
		i = j
	}
	return lhs, i
}

func (parser *Parser) makeNewExpression(leftExpr interface{}, rightExpr interface{}, op *ExpressionOp) *ExpressionStm {
	_, leftIsExpressionTerm := leftExpr.(*ExpressionTerm)
	_, rightIsExpressionTerm := rightExpr.(*ExpressionTerm)
	ret := new(ExpressionStm)
	if leftIsExpressionTerm {
		ret.LeftExpr = leftExpr.(*ExpressionTerm)
	} else {
		ret.LeftExpr = leftExpr.(*ExpressionStm)
	}
	if rightIsExpressionTerm {
		ret.RightExpr = rightExpr.(*ExpressionTerm)
	} else {
		ret.RightExpr = rightExpr.(*ExpressionStm)
	}
	ret.Op = op
	return ret
}

func (parser *Parser) parseExpressionTerm() (expr *ExpressionTerm, err error) {
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch token.Tp {
	case IDENT:
		// Must be identifier
		parser.UnReadToken()
		expr, err = parser.parseIdentifierExpressionTerm()
	case WORD:
		// Must be function call or identifier
		expr, err = parser.parseFunctionCallOrIdentifierStm()
	case VALUE:
		// Must be literal
		parser.UnReadToken()
		expr, err = parser.parseLiteralExpressionTerm()
	// case lexer.NOT, lexer.EXIST:
	//	// Must be not exist subquery
	//	parser.UnReadToken()
	//	expr, err = parser.parseExistsSubQueryExpression()
	case MINUS:
		parser.UnReadToken()
		expr, err = parser.parseUnaryExpressionTerm()
	case LEFTBRACKET:
		expr, err = parser.parseSubExpressionTerm()
	default:
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return
}

func (parser *Parser) parseUnaryExpressionTerm() (expr *ExpressionTerm, err error) {
	if !parser.matchTokenTypes(false, MINUS) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	expr, err = parser.parseExpressionTerm()
	if err != nil {
		return nil, err
	}
	expr.UnaryOp = NegativeUnaryOpTp
	return
}

func (parser *Parser) parseFunctionCallOrIdentifierStm() (expr *ExpressionTerm, err error) {
	if parser.matchTokenTypes(true, LEFTBRACKET) {
		// Must be functionCall
		// Back to functionName position.
		parser.UnReadToken()
		parser.UnReadToken()
		expr, err = parser.parseFunctionCallExpression()
	} else {
		parser.UnReadToken()
		expr, err = parser.parseIdentifierExpressionTerm()
	}
	return
}

func (parser *Parser) parseSubExpressionTerm() (expr *ExpressionTerm, err error) {
	exprTerm, err := parser.resolveExpression()
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &ExpressionTerm{
		UnaryOp:      NoneUnaryOpTp,
		Tp:           SubExpressionTermTP,
		RealExprTerm: exprTerm,
	}, nil
}

func isTokenAOpe(token Token) bool {
	switch token.Tp {
	case PLUS, MINUS, MUL, DIVIDE, MOD, EQUAL, IS,
		NOTEQUAL, GREAT, GREATEQUAL, LESS, LESSEQUAL, AND,
		OR:
		return true
	default:
		return false
	}
}

func (parser *Parser) parseLiteralExpressionTerm() (*ExpressionTerm, error) {
	value, ok := parser.parseValue(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &ExpressionTerm{
		UnaryOp:      NoneUnaryOpTp,
		Tp:           LiteralExpressionTermTP,
		RealExprTerm: LiteralExpressionStm(ColumnValue(value)),
	}, nil
}

func (parser *Parser) parseIdentifierExpressionTerm() (*ExpressionTerm, error) {
	name, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &ExpressionTerm{
		UnaryOp:      NoneUnaryOpTp,
		Tp:           IdentifierExpressionTermTP,
		RealExprTerm: IdentifierExpression(name),
	}, nil
}

func (parser *Parser) parseFunctionCallExpression() (*ExpressionTerm, error) {
	funcName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	var params []*ExpressionStm
	for {
		paramExpression, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		params = append(params, paramExpression)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &ExpressionTerm{
		UnaryOp: NoneUnaryOpTp,
		Tp:      FuncCallExpressionTermTP,
		RealExprTerm: FunctionCallExpressionStm{
			FuncName: string(funcName),
			Params:   params,
		},
	}, nil
}

//func (parser *Parser) parseInOrLikeExpressions(token lexer.Token, leftExpr *ast.ExpressionStm) (expr *ast.ExpressionStm, err error) {
//	switch token.Tp {
//	case lexer.NOT:
//		if parser.matchTokenTypes(true, lexer.IN) {
//			expr, err = parser.parseInExpression(false, leftExpr)
//		} else if parser.matchTokenTypes(true, lexer.LIKE) {
//			expr, err = parser.parseLikeExpression(false, leftExpr)
//		}
//	case lexer.IN:
//		expr, err = parser.parseInExpression(true, leftExpr)
//	case lexer.LIKE:
//		expr, err = parser.parseLikeExpression(true, leftExpr)
//	default:
//		return nil, parser.MakeSyntaxError(parser.pos)
//	}
//	return
//}
//
//func (parser *Parser) parseInExpression(in bool, leftExpr *ast.ExpressionStm) (*ast.ExpressionStm, error) {
//	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
//		return nil, parser.MakeSyntaxError(parser.pos-1)
//	}
//	if parser.matchTokenTypes(true, lexer.SELECT) {
//		parser.UnReadToken()
//		query, err := parser.resolveSelectStm(false)
//		if err != nil {
//			return nil, err
//		}
//		if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
//			return nil, parser.MakeSyntaxError(parser.pos-1)
//		}
//		return &ast.ExpressionStm{
//			Tp: ast.ExpressionInSubqueryTp,
//			Expr: ast.ExpressionInSubQueryStm{
//				Expr:     leftExpr,
//				In:       in,
//				SubQuery: query,
//			},
//		}, nil
//	}
//	var exprs []*ast.ExpressionStm
//	for {
//		expr, err := parser.resolveExpression()
//		if err != nil {
//			return nil, err
//		}
//		exprs = append(exprs, expr)
//		if !parser.matchTokenTypes(true, lexer.COMMA) {
//			break
//		}
//	}
//	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
//		return nil, parser.MakeSyntaxError(parser.pos-1)
//	}
//	return &ast.ExpressionStm{
//		Tp: ast.ExpressionInExpressionsTp,
//		Expr: ast.ExpressionInExpressionsStm{
//			Expr:  leftExpr,
//			In:    in,
//			Values: exprs,
//		},
//	}, nil
//}
//
//func (parser *Parser) parseLikeExpression(like bool, leftExpr *ast.ExpressionStm) (*ast.ExpressionStm, error) {
//	rightExpr, err := parser.parseExpressionTerm()
//	if err != nil {
//		return nil, err
//	}
//	return &ast.ExpressionStm{
//		Tp: ast.ExpressionLikeVariableTp,
//		Expr: ast.ExpressionLikeVariableStm{
//			Expr:     leftExpr,
//			Like:     like,
//			Variable: rightExpr,
//		},
//	}, nil
//}
//
//func (parser *Parser) parseExpressionOpeExpressionStm(token lexer.Token, leftExpr *ast.ExpressionStm) (*ast.ExpressionStm, error) {
//	if !isTokenAOpe(token) {
//		return leftExpr, nil
//	}
//	ope := token.Tp
//	if token.Tp == lexer.IS && parser.matchTokenTypes(true, lexer.NOT) {
//		// Need to check whether follow a NOT.
//		ope = ast.OperationISNotTp
//	}
//	rightExpr, err := parser.resolveExpression()
//	if err != nil {
//		return nil, err
//	}
//	return &ast.ExpressionStm{
//		Tp: ast.ExpressionOpeExpressionTp,
//		Expr: ast.ExpressionOpeExpressionStm{
//			Expr1: leftExpr,
//			Ope:   ope,
//			Expr2: rightExpr,
//		},
//	}, nil
//}

//func (parser *Parser) parseExistsSubQueryExpression() (*ast.ExpressionStm, error) {
//	nonExists := parser.matchTokenTypes(true, lexer.NOT)
//	if !parser.matchTokenTypes(true, lexer.EXIST) {
//		return nil, parser.MakeSyntaxError(parser.pos)
//	}
//	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
//		return nil, parser.MakeSyntaxError(parser.pos-1)
//	}
//	query, err := parser.resolveSelectStm(false)
//	if err != nil {
//		return nil, err
//	}
//	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
//		return nil, parser.MakeSyntaxError(parser.pos-1)
//	}
//	return &ast.ExpressionStm{
//		Tp: ast.VariableExpressionTp,
//		Expr: ast.VariableExpressionStm{
//			Tp: ast.ExistsSubQueryExpressionTp,
//			Variable: ast.ExistsSubQueryExpressionStm{
//				Exists:   !nonExists,
//				SubQuery: query,
//			},
//		},
//	}, nil
//}
