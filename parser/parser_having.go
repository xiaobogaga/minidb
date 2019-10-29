package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Having WhereStm

func (parser *Parser) parseHavingStm() (ast.HavingStm, error) {
	if !parser.matchTokenTypes(true, lexer.HAVING) {
		return nil, nil
	}
	whereStm, err := parser.resolveWhereStm()
	if err != nil {
		return nil, err
	}
	if whereStm == nil {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	return ast.HavingStm(whereStm), nil
}
