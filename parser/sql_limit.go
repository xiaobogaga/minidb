package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

func (parser *Parser) resolveLimit(ifNotRollback bool) (*ast.LimitStm, error) {
	// LIMIT count
 	if !parser.matchTokenType(lexer.LIMIT, ifNotRollback) {
 		return nil, nil
	}
	ret, err := parser.parseIntValue(false)
	if err != nil {
		return nil, err
	}
	return &ast.LimitStm{Count: ret}, nil
}
