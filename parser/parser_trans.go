package parser

func (parser *Parser) ParseTransStm() (TransStm, error) {
	token, remain := parser.NextToken()
	if !remain {
		return "", parser.MakeSyntaxError(parser.pos)
	}
	stm := BeginStm
	switch token.Tp {
	case BEGIN:
		stm = BeginStm
	case ROLLBACK:
		stm = RollbackStm
	case COMMIT:
		stm = CommitStm
	default:
		return "", parser.MakeSyntaxError(parser.pos - 1)
	}
	if !parser.matchTokenTypes(true, SEMICOLON) {
		return "", parser.MakeSyntaxError(parser.pos)
	}
	return stm, nil
}
