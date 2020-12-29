package parser

// An assignment statement is like:
// * col_name = expressionSta

func (parser *Parser) parseAssignmentStm() (*AssignmentStm, error) {
	colName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if !parser.matchTokenTypes(false, EQUAL) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return nil, err
	}
	return &AssignmentStm{
		ColName: string(colName),
		Value:   expressionStm,
	}, nil
}
