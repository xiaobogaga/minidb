package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const (
	DropStmErr = ParseError("drop statement error")
	DropTableStmErr = ParseError("drop table statement error")
	DropDatabaseStmErr = ParseError("drop database statement error")
)

func (parser *Parser) resolveDropStm() (stm ast.Stm, err error) {
	// drop table|database IF EXIST ident|word ;
	if !parser.hasNext() {
		return nil, TokensEndErr
	}
	t := parser.getToken()
	switch t.Tp {
	case lexer.TABLE:
		stm, err = parser.parseDropTableStm()
	case lexer.DATABASE:
		stm, err = parser.parseDropDatabaseStm()
	default:
		err = DropStmErr
	}
	if err != nil {
		return nil, DropStmErr.Wrapper(err)
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, DropStmErr
	}
	return stm, nil
}

func (parser *Parser) parseDropTableStm() (*ast.DropTableStm, error) {
	// IF EXIST ident|word, ident|word ;
	ifExist := parser.matchTokenTypes(true, lexer.IF, lexer.EXIST)
	var tableNames []string
	for {
		name, ret := parser.parseIdentOrWord(false)
		if !ret {
			return nil, DropTableStmErr
		}
		tableNames = append(tableNames, name)
		if !parser.matchTokenType(lexer.COMMA, true) {
			break
		}
	}
	dropTableStm := ast.NewDropTableStm(ifExist, tableNames...)
	return dropTableStm, nil
}

func (parser *Parser) parseDropDatabaseStm() (*ast.DropDatabaseStm, error) {
	// IF EXIST ident|word ;
	ifExist := parser.matchTokenTypes(true, lexer.IF, lexer.EXIST)
	name, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, DropDatabaseStmErr
	}
	dropDatabaseStm := ast.NewDropDatabaseStm(name, ifExist)
	return dropDatabaseStm, nil
}

