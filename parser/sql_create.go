package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const (
	CreateStmErr = ParseError("create statement err")
	CreateTableStmErr = ParseError("create table statement err")
	CreateDatabaseStmErr = ParseError("create database statement err")
	TableNameEmptyErr = ParseError("table name cannot be empty")
	WrongTableNameFormatErr = ParseError("wrong table name format")
	ParseTempErr = ParseError("temp parse error")
	ParseColumnErr = ParseError("parse column err")
	TokensEndErr = ParseError("tokens has ended")
	NotSemicolonErr = ParseError("not a semicolon")
	NotLeftBracketErr = ParseError("not a (")
	NotRightBracketErr = ParseError("not a )")
	NotCommaErr = ParseError("not a ,")
	UnKnownTypeErr = ParseError("unknown column type")
	NeedLeftBracketErr = ParseError("need a leftBracket")
)

func (parser *Parser) resolveCreateStm() (stm ast.Stm, err error) {
	if !parser.hasNext() {
		return nil, CreateStmErr.Wrapper(TokensEndErr)
	}
	token := parser.getToken()
	switch token.Tp {
	case lexer.TABLE:
		stm, err = parser.parseCreateTableStm()
	case lexer.DATABASE:
		stm, err = parser.parseCreateDatabaseStm()
	default:
		err = CreateStmErr
	}
	if err != nil {
		return nil, CreateStmErr.Wrapper(err)
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, CreateStmErr.Wrapper(NotSemicolonErr)
	}
	return stm, nil
}

func (parser *Parser) parseCreateTableStm() (*ast.CreateTableStm, error) {
	// create table [IFNOTEXIST] ident|word (COLDEF[,COLDEF...]);
	ifNotExist := parser.matchTokenTypes(true, lexer.IF, lexer.NOT, lexer.EXIST)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongTableNameFormatErr
	}
	createTableStm := ast.NewCreateTableStm(tableName, ifNotExist)
	if createTableStm.IsTableNameEmpty() {
		return nil, TableNameEmptyErr
	}
	if !parser.matchTokenType(lexer.LEFTBRACKET, false) {
		return nil, NeedLeftBracketErr
	}
	for {
		col, err := parser.parseColumnDef()
		if err != nil {
			return nil, err
		}
		createTableStm.AppendCol(col)
		if !parser.matchTokenType(lexer.COMMA, true) {
			break
		}
	}
	if parser.matchTokenType(lexer.RIGHTBRACKET, false) {
		return createTableStm, nil
	} else {
		return nil, CreateTableStmErr
	}
}

func (parser *Parser) parseCreateDatabaseStm() (*ast.CreateDatabaseStm, error) {
	// create if not exist database ident|word ;
	ifNotExist := parser.matchTokenTypes(true, lexer.IF, lexer.NOT, lexer.EXIST)
	databaseName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, CreateDatabaseStmErr
	}
	stm := ast.NewCreateDatabaseStm(databaseName, ifNotExist)
	return stm, nil
}