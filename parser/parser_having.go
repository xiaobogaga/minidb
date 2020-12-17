package parser

// Having WhereStm

func (parser *Parser) parseHavingStm() (HavingStm, error) {
	if !parser.matchTokenTypes(true, HAVING) {
		return nil, nil
	}
	whereStm, err := parser.resolveWhereStm()
	if err != nil {
		return nil, err
	}
	if whereStm == nil {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	return HavingStm(whereStm), nil
}
