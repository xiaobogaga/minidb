package parser

var emptyGroupByStm = GroupByStm{}

// group by {expressions [asc|desc]}...

func (parser *Parser) ParseGroupByStm() (*GroupByStm, error) {
	if !parser.matchTokenTypes(true, GROUP, BY) {
		return nil, nil
	}
	var expressionStms []*ExpressionStm
	for {
		expressionStm, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		expressionStms = append(expressionStms, expressionStm)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	groupBy := GroupByStm(expressionStms)
	return &groupBy, nil
}

// Return desc if matched, or return asc (true as default) otherwise.
func (parser *Parser) parseAscOrDesc() bool {
	if parser.matchTokenTypes(true, ASC) {
		return true
	}
	return !parser.matchTokenTypes(true, DESC)
}
