package parser

// Having WhereStm

func (parser *Parser) parseHavingStm() (HavingStm, error) {
	if !parser.matchTokenTypes(true, HAVING) {
		return nil, nil
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return expressionStm, nil
}
