package parser

import "strings"

func (parser *Parser) resolveUseStm() (Stm, error) {
	if !parser.matchTokenTypes(false, USE) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	databaseName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return UseDatabaseStm{DatabaseName: string(databaseName)}, nil
}

func (parser *Parser) resolveShowStm() (Stm, error) {
	if !parser.matchTokenTypes(false, SHOW) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	databaseOrTable, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	stm := ShowStm{}
	switch strings.ToUpper(string(databaseOrTable)) {
	case "DATABASE":
		stm.TP = ShowDatabaseTP
	case "TABLE":
		stm.TP = ShowTableTP
	default:
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return stm, nil
}
