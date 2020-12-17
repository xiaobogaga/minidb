package parser

// A where statement is like: Where expressionStm

func (parser *Parser) resolveWhereStm() (WhereStm, error) {
	if !parser.matchTokenTypes(true, WHERE) {
		return nil, nil
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return expressionStm, nil
}
