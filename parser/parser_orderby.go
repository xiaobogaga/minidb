package parser

// order by expressions,...

func (parser *Parser) ParseOrderByStm() (*OrderByStm, error) {
	if !parser.matchTokenTypes(true, ORDER, BY) {
		return nil, nil
	}
	var OrderedExpressionStms []*OrderedExpressionStm
	for {
		expressionStm, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		asc := parser.parseAscOrDesc()
		OrderedExpressionStms = append(OrderedExpressionStms, &OrderedExpressionStm{
			Expression: expressionStm,
			Asc:        asc,
		})
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	return &OrderByStm{Expressions: OrderedExpressionStms}, nil
}
