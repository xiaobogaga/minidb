package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

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
func (parser *Parser) resolveExpression() (expr *ast.ExpressionStm, err error) {
	exprTerm, err := parser.parseExpressionTerm()
	if err != nil {
		return nil, err
	}
	var exprs []*ast.ExpressionTerm
	exprs = append(exprs, exprTerm)
	var ops []ast.ExpressionOp
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

func (parser *Parser) LexerOpToExpressionOp(op lexer.TokenType) ast.ExpressionOp {
	switch op {
	case lexer.ADD:
		return ast.OperationAdd
	case lexer.MINUS:
		return ast.OperationMinus
	case lexer.MUL:
		return ast.OperationMul
	case lexer.DIVIDE:
		return ast.OperationDivide
	case lexer.MOD:
		return ast.OperationMod
	case lexer.EQUAL:
		return ast.OperationEqual
	case lexer.IS:
		return ast.OperationIs
	case lexer.NOTEQUAL:
		return ast.OperationNotEqual
	case lexer.GREAT:
		return ast.OperationGreat
	case lexer.GREATEQUAL:
		return ast.OperationGreatEqual
	case lexer.LESS:
		return ast.OperationLess
	case lexer.LESSEQUAL:
		return ast.OperationLessEqual
	case lexer.AND:
		return ast.OperationAnd
	case lexer.OR:
		return ast.OperationOr
	case lexer.OR + 1:
		return ast.OperationISNot
	default:
		panic("unknown op type")
	}
}

func (parser *Parser) buildExpressionsTree(ops []ast.ExpressionOp, exprTerms []*ast.ExpressionTerm) *ast.ExpressionStm {
	if len(ops) == 0 {
		return &ast.ExpressionStm{LeftExpr: exprTerms[0]}
	}
	if len(ops) == 1 {
		return &ast.ExpressionStm{LeftExpr: exprTerms[0], Op: ops[0], RightExpr: exprTerms[1]}
	}
	expressionStack := make([]interface{}, 0, len(exprTerms))
	for _, exprTerm := range exprTerms {
		expressionStack = append(expressionStack, exprTerm)
	}
	for i := 2; len(expressionStack) > 2; i = i % len(expressionStack) {
		nextOp := ops[i-1]
		lastOp := ops[i-2]
		if lastOp.Priority >= nextOp.Priority {
			// We can merge last two expression to a new expression node.
			lastLeftExpression, lastRightExpression := expressionStack[i-2], expressionStack[i-1]
			newExpr := parser.makeNewExpression(lastLeftExpression, lastRightExpression, lastOp)
			expressionStack = append(expressionStack[:i-1], expressionStack[i:]...)
			expressionStack[i-2] = newExpr
			ops = append(ops[:i-2], ops[i-1:]...)
			continue
		}
		i++
	}
	return parser.makeNewExpression(expressionStack[0], expressionStack[1], ops[0])
}

func (parser *Parser) makeNewExpression(leftExpr interface{}, rightExpr interface{}, op ast.ExpressionOp) *ast.ExpressionStm {
	_, leftIsExpressionTerm := leftExpr.(*ast.ExpressionTerm)
	_, rightIsExpressionTerm := rightExpr.(*ast.ExpressionTerm)
	ret := new(ast.ExpressionStm)
	if leftIsExpressionTerm {
		ret.LeftExpr = leftExpr.(*ast.ExpressionTerm)
	} else {
		ret.LeftExpr = leftExpr.(*ast.ExpressionStm)
	}
	if rightIsExpressionTerm {
		ret.RightExpr = rightExpr.(*ast.ExpressionTerm)
	} else {
		ret.RightExpr = rightExpr.(*ast.ExpressionStm)
	}
	ret.Op = op
	return ret
}

func (parser *Parser) parseExpressionTerm() (expr *ast.ExpressionTerm, err error) {
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch token.Tp {
	case lexer.IDENT:
		// Must be identifier
		parser.UnReadToken()
		expr, err = parser.parseIdentifierExpressionTerm()
	case lexer.WORD:
		// Must be function call or identifier
		expr, err = parser.parseFunctionCallOrIdentifierStm()
	case lexer.VALUE:
		// Must be literal
		parser.UnReadToken()
		expr, err = parser.parseLiteralExpressionTerm()
	// case lexer.NOT, lexer.EXIST:
	//	// Must be not exist subquery
	//	parser.UnReadToken()
	//	expr, err = parser.parseExistsSubQueryExpression()
	case lexer.MINUS:
		parser.UnReadToken()
		expr, err = parser.parseUnaryExpressionTerm()
	case lexer.LEFTBRACKET:
		expr, err = parser.parseSubExpressionTerm()
	default:
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	return
}

func (parser *Parser) parseUnaryExpressionTerm() (expr *ast.ExpressionTerm, err error) {
	if parser.matchTokenTypes(false, lexer.MINUS) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	exprTerm, err := parser.parseExpressionTerm()
	exprTerm.UnaryOp = ast.NegativeUnaryOpTp
	return
}

func (parser *Parser) parseFunctionCallOrIdentifierStm() (expr *ast.ExpressionTerm, err error) {
	if parser.matchTokenTypes(true, lexer.LEFTBRACKET) {
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

func (parser *Parser) parseSubExpressionTerm() (expr *ast.ExpressionTerm, err error) {
	exprTerm, err := parser.resolveExpression()
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.ExpressionTerm{
		Tp:           ast.SubExpressionTermTP,
		RealExprTerm: exprTerm,
	}, nil
}

func isTokenAOpe(token lexer.Token) bool {
	switch token.Tp {
	case lexer.ADD, lexer.MINUS, lexer.MUL, lexer.DIVIDE, lexer.MOD, lexer.EQUAL, lexer.IS,
		lexer.NOTEQUAL, lexer.GREAT, lexer.GREATEQUAL, lexer.LESS, lexer.LESSEQUAL, lexer.AND,
		lexer.OR:
		return true
	default:
		return false
	}
}

func (parser *Parser) parseLiteralExpressionTerm() (*ast.ExpressionTerm, error) {
	value, ok := parser.parseValue(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.ExpressionTerm{
		Tp:           ast.LiteralExpressionTermTP,
		RealExprTerm: ast.LiteralExpressionStm(value),
	}, nil
}

func (parser *Parser) parseIdentifierExpressionTerm() (*ast.ExpressionTerm, error) {
	name, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.ExpressionTerm{
		Tp:           ast.IdentifierExpressionTermTP,
		RealExprTerm: ast.IdentifierExpression(name),
	}, nil
}

func (parser *Parser) parseFunctionCallExpression() (*ast.ExpressionTerm, error) {
	funcName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var params []*ast.ExpressionStm
	for {
		paramExpression, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		params = append(params, paramExpression)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.ExpressionTerm{
		Tp: ast.FuncCallExpressionTermTP,
		RealExprTerm: ast.FunctionCallExpressionStm{
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
//		return nil, parser.MakeSyntaxError(1, parser.pos)
//	}
//	return
//}
//
//func (parser *Parser) parseInExpression(in bool, leftExpr *ast.ExpressionStm) (*ast.ExpressionStm, error) {
//	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
//		return nil, parser.MakeSyntaxError(1, parser.pos-1)
//	}
//	if parser.matchTokenTypes(true, lexer.SELECT) {
//		parser.UnReadToken()
//		query, err := parser.resolveSelectStm(false)
//		if err != nil {
//			return nil, err
//		}
//		if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
//			return nil, parser.MakeSyntaxError(1, parser.pos-1)
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
//		return nil, parser.MakeSyntaxError(1, parser.pos-1)
//	}
//	return &ast.ExpressionStm{
//		Tp: ast.ExpressionInExpressionsTp,
//		Expr: ast.ExpressionInExpressionsStm{
//			Expr:  leftExpr,
//			In:    in,
//			Exprs: exprs,
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
//		return nil, parser.MakeSyntaxError(1, parser.pos)
//	}
//	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
//		return nil, parser.MakeSyntaxError(1, parser.pos-1)
//	}
//	query, err := parser.resolveSelectStm(false)
//	if err != nil {
//		return nil, err
//	}
//	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
//		return nil, parser.MakeSyntaxError(1, parser.pos-1)
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
