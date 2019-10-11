package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongWhereStmFormatErr = ParseError("wrong where statement format error")

func (parser *Parser) resolveWhereStm(ifNotRollback bool) (*ast.WhereStm, error) {
	// WhereStm: [where ident|word==value1[ [Relation, ident|word Condition value2...]]
	// WhereStm: [Where expression[,expression]+
	if !parser.matchTokenType(lexer.WHERE, ifNotRollback) {
		return nil, nil
	}
	expressionStms, err := parser.resolveExpression(false)
	if err != nil {
		return nil, WrongWhereStmFormatErr.Wrapper(err)
	}
	return &ast.WhereStm{ExpressionStms: expressionStms}, nil
}
