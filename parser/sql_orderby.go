package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongOrderbyFormatErr = ParseError("wrong order by format")

func (parser *Parser) resolveOrderBy(ifNotRollback bool) (*ast.OrderByStm, error) {
	// Order by col1[, col2,...]
	if !parser.matchTokenType(lexer.ORDER, ifNotRollback) {
		return nil, nil
	}
	if !parser.matchTokenType(lexer.BY, false) {
		return nil, WrongOrderbyFormatErr
	}
	var orderBy []string
	for {
		colName, ret := parser.parseIdentOrWord(false)
		if !ret {
			return nil, WrongOrderbyFormatErr
		}
		orderBy = append(orderBy, colName)
		if parser.matchTokenType(lexer.COMMA, true) {
			continue
		} else {
			break
		}
	}
	return &ast.OrderByStm{Cols: orderBy}, nil
}
