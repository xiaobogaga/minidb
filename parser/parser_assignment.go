package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var emptyAssignmentStm = ast.AssignmentStm{}

// An assignment statement is like:
// * col_name = expressionSta

func (parser *Parser) parseAssignmentStm() (ast.AssignmentStm, error) {
	colName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyAssignmentStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.EQUAL) {
		return emptyAssignmentStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return emptyAssignmentStm, err
	}
	return ast.AssignmentStm{
		ColName: string(colName),
		Value:   expressionStm,
	}, nil
}
