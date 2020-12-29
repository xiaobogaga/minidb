package parser

// using (col,...)
func (parser *Parser) parseUsingJoinSpec() (JoinSpecification, error) {
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return emptyJoinSepc, parser.MakeSyntaxError(parser.pos - 1)
	}
	var cols []string
	for {
		col, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyJoinSepc, parser.MakeSyntaxError(parser.pos - 1)
		}
		cols = append(cols, string(col))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return emptyJoinSepc, parser.MakeSyntaxError(parser.pos - 1)
	}
	return JoinSpecification{Tp: JoinSpecificationUsing, Condition: cols}, nil
}

// on whereStm
func (parser *Parser) parseOnJoinSpec() (JoinSpecification, error) {
	if !parser.matchTokenTypes(false, ON) {
		return emptyJoinSepc, parser.MakeSyntaxError(parser.pos - 1)
	}
	whereStm, err := parser.ResolveWhereStm()
	if err != nil {
		return emptyJoinSepc, err
	}
	return JoinSpecification{Tp: JoinSpecificationON, Condition: whereStm}, nil
}
