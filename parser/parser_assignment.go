package parser

var emptyAssignmentStm = AssignmentStm{}

// An assignment statement is like:
// * col_name = expressionSta

func (parser *Parser) parseAssignmentStm() (AssignmentStm, error) {
	colName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyAssignmentStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, EQUAL) {
		return emptyAssignmentStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	expressionStm, err := parser.resolveExpression()
	if err != nil {
		return emptyAssignmentStm, err
	}
	return AssignmentStm{
		ColName: string(colName),
		Value:   expressionStm,
	}, nil
}
