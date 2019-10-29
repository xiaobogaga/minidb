package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// order by expressions [asc|desc],...

func (parser *Parser) parseOrderByStm() (*ast.OrderByStm, error) {
	if !parser.matchTokenTypes(true, lexer.ORDER, lexer.BY) {
		return nil, nil
	}
	var expressionStms []*ast.ExpressionStm
	var asc []bool
	for {
		expressionStm, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		expressionStms = append(expressionStms, expressionStm)
		asc = append(asc, parser.parseAscOrDesc())
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	return &ast.OrderByStm{
		Expressions: expressionStms,
		Asc:         asc,
	}, nil
}
