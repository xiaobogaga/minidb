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
	return &ast.GroupByStm{
		Expressions: expressionStms,
		Asc:         asc,
	}, nil
}

// Return desc if matched, or return asc (true as default) otherwise.
func (parser *Parser) parseAscOrDesc() bool {
	return !parser.matchTokenTypes(true, lexer.DESC)
}
