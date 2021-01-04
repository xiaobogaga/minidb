package parser

func (parser *Parser) resolveUseStm() (Stm, error) {
	if !parser.matchTokenTypes(false, USE) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	databaseName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &UseDatabaseStm{DatabaseName: string(databaseName)}, nil
}

func (parser *Parser) resolveShowStm() (Stm, error) {
	if !parser.matchTokenTypes(false, SHOW) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	databaseOrTable, ret := parser.NextToken()
	if !ret {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	stm := &ShowStm{}
	switch databaseOrTable.Tp {
	case DATABASES:
		stm.TP = ShowDatabaseTP
	case TABLES:
		stm.TP = ShowTableTP
	case CREATE:
		// Show create table stm.
		if !parser.matchTokenTypes(false, TABLE) {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		tableName, success := parser.parseIdentOrWord(false)
		if !success {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		stm.TP = ShowCreateTableTP
		stm.Table = string(tableName)
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return stm, nil
}
