package parser

// using (col,...)
func (parser *Parser) parseUsingJoinSpec() (*JoinSpecification, error) {
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	var cols []string
	for {
		col, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		cols = append(cols, string(col))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &JoinSpecification{Tp: JoinSpecificationUsing, Condition: cols}, nil
}

// on whereStm
func (parser *Parser) parseOnJoinSpec() (*JoinSpecification, error) {
	if !parser.matchTokenTypes(false, ON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	whereStm, err := parser.resolveExpression()
	if err != nil {
		return nil, err
	}
	return &JoinSpecification{Tp: JoinSpecificationON, Condition: whereStm}, nil
}
