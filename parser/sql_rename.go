package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongRenameStm = ParseError("wrong rename statement format")

func (parser *Parser) resolveRename() (*ast.RenameStm, error) {
	// Rename table|database tb1 To tb2
	isTable := parser.matchTokenType(lexer.TABLE, true)
	if !isTable {
		if !parser.matchTokenType(lexer.DATABASE, false) {
			return nil, WrongRenameStm
		}
	}
	origName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongRenameStm
	}
	if !parser.matchTokenType(lexer.TO, false) {
		return nil, WrongRenameStm
	}
	modifiedName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongRenameStm
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, WrongRenameStm
	}
	tp := lexer.TABLE
	if !isTable {
		tp = lexer.DATABASE
	}
	return &ast.RenameStm{OrigNames: origName, ModifiedNames: modifiedName, Tp: tp}, nil
}
