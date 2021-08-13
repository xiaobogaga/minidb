package parser

// A where statement is like: Where expressionStm

func (parser *Parser) ResolveWhereStm() (WhereStm, error) {
	if !parser.matchTokenTypes(true, WHERE) {
		return nil, nil
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return expressionStm, nil
}
