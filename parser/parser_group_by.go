package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var emptyGroupByStm = ast.GroupByStm{}

// group by {expressions [asc|desc]}...

func (parser *Parser) parseGroupByStm() (*ast.GroupByStm, error) {
	if !parser.matchTokenTypes(true, lexer.GROUP, lexer.BY) {
		return nil, nil
	}
	var expressionStms []*ast.ExpressionStm
	for {
		expressionStm, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		expressionStms = append(expressionStms, expressionStm)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	groupBy := ast.GroupByStm(expressionStms)
	return &groupBy, nil
}

// Return desc if matched, or return asc (true as default) otherwise.
func (parser *Parser) parseAscOrDesc() bool {
	return !parser.matchTokenTypes(true, lexer.DESC)
}
