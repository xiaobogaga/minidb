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
	var exprs []ast.ExpressionTerm
	exprs = append(exprs, *exprTerm)
	var ops []ast.ExpressionOpTP
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
		ops = append(ops, ast.ExpressionOpTP(token.Tp))
		exprs = append(exprs, *rightExprTerm)
	}
	return &ast.ExpressionStm{
		ExprTerms: exprs,
		Ops:       ops,
	}, nil
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
