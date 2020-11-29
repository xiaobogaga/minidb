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
	var OrderedExpressionStms []*ast.OrderedExpressionStm
	for {
		expressionStm, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		asc := parser.parseAscOrDesc()
		OrderedExpressionStms = append(OrderedExpressionStms, &ast.OrderedExpressionStm{
			Expression: expressionStm,
			Asc:        asc,
		})
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	return &ast.OrderByStm{Expressions: OrderedExpressionStms}, nil
}
