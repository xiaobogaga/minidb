package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Drop statement can be drop table statement or drop database statement.
// Drop database statement is like:
// * drop {database | schema} [if exists] db_name;
// Drop table statement is like:
// * drop table [if exists] tb_name[,tb_name...] [RESTRICT|CASCADE];

// parseDropStm parses a drop statement and return it.
func (parser *Parser) parseDropStm() (stm ast.Stm, err error) {
	if !parser.matchTokenTypes(false, lexer.DROP) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	t, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch t.Tp {
	case lexer.TABLE:
		stm, err = parser.parseDropTableStm()
	case lexer.DATABASE, lexer.SCHEMA:
		stm, err = parser.parseDropDatabaseStm()
	default:
		err = parser.MakeSyntaxError(1, parser.pos)
	}
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return stm, nil
}

// Drop table statement is like:
// * drop table [if exists] tb_name[,tb_name...] [RESTRICT|CASCADE];
func (parser *Parser) parseDropTableStm() (ast.Stm, error) {
	ifExist := parser.matchTokenTypes(true, lexer.IF, lexer.EXIST)
	var tableNames []string
	for {
		name, ret := parser.parseIdentOrWord(false)
		if !ret {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		tableNames = append(tableNames, string(name))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	dropTableStm := ast.NewDropTableStm(ifExist, tableNames...)
	parser.matchTokenTypes(true, lexer.RESTRICT)
	parser.matchTokenTypes(true, lexer.CASCADE)
	return dropTableStm, nil
}

// Drop database statement is like:
// * drop {database | schema} [if exists] db_name;
func (parser *Parser) parseDropDatabaseStm() (ast.Stm, error) {
	ifExist := parser.matchTokenTypes(true, lexer.IF, lexer.EXIST)
	name, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	dropDatabaseStm := ast.NewDropDatabaseStm(string(name), ifExist)
	return dropDatabaseStm, nil
}
