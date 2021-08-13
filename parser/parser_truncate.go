package parser

// Truncate table statement is like:
// * truncate [table] tb_name

func (parser *Parser) resolveTruncate() (Stm, error) {
	if !parser.matchTokenTypes(false, TRUNCATE) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	parser.matchTokenTypes(true, TABLE)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &TruncateStm{TableName: string(tableName)}, nil
}
