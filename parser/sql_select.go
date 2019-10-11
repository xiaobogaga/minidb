package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongSelectStmFormatErr = ParseError("wrong select statement error")

func (parser *Parser) resolveSelectStm() (*ast.SelectStm, error) {
	// select [*|expression[,expression]+] from ident|word WhereStm OrderByStm LimitStm;
	if parser.matchTokenType(lexer.STAR, true) {
		return parser.resolveStarSelectStm()
	}
	return parser.resolveExpressionSelectStm()
}

func (parser *Parser) resolveStarSelectStm() (*ast.SelectStm, error) {
	// * from ident|word WhereStm OrderByStm LimitStm;
	if !parser.matchTokenType(lexer.FROM, false) {
		return nil, WrongSelectStmFormatErr
	}
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongSelectStmFormatErr
	}
	whereStm, err := parser.resolveWhereStm(true)
	if err != nil {
		return nil, WrongSelectStmFormatErr.Wrapper(err)
	}
	orderByStm, err := parser.resolveOrderBy(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	limitStm, err := parser.resolveLimit(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, WrongDeleteStmErr
	}
	return &ast.SelectStm{TableName:tableName, WhereStm: whereStm, OrderByStm: orderByStm, LimitStm: limitStm}, nil
}

func (parser *Parser) resolveExpressionSelectStm() (*ast.SelectStm, error) {
	// expression[,expression]+ from ident|word WhereStm OrderByStm LimitStm;
	var expressions []ast.Stm
	for {
		expressionStm, err := parser.resolveExpression(true)
		if err != nil {
			return nil, WrongSelectStmFormatErr.Wrapper(err)
		}
		expressions = append(expressions, expressionStm)
		if !parser.matchTokenType(lexer.COMMA, true) {
			break
		}
	}
	if !parser.matchTokenType(lexer.FROM, false) {
		return nil, WrongSelectStmFormatErr
	}
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongSelectStmFormatErr
	}
	whereStm, err := parser.resolveWhereStm(true)
	if err != nil {
		return nil, WrongSelectStmFormatErr.Wrapper(err)
	}
	orderByStm, err := parser.resolveOrderBy(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	limitStm, err := parser.resolveLimit(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, WrongDeleteStmErr
	}
	return &ast.SelectStm{Expressions: expressions, TableName:tableName, WhereStm: whereStm, OrderByStm: orderByStm, LimitStm: limitStm}, nil
}
