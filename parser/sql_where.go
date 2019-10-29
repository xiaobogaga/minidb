package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// A where statement is like: Where expressionStm

func (parser *Parser) resolveWhereStm() (ast.WhereStm, error) {
	if !parser.matchTokenTypes(true, lexer.WHERE) {
		return nil, nil
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return expressionStm, nil
}
