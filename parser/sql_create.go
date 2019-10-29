package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Create Statement can be create table statement or create database statement.
// For create table statement, it supports:
// * create table [if not exist] tb_name like orig_tab_name;
// * create table [if not exist] tb_name2 (Column_Def..., Index_Def..., Constraint_Def...) [engine=value] [[Default | character set = value] | [Default | collate = value]];
// * create table [if not exist] as selectStatement;
// For create database statement, if supports:
// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];

// Diff with mysql:
// Create table statement:
// * Doesn't support temporary table.
// * Doesn't support ignore or replace.
// * Doesn't support spatial or fulltext index.
// * Doesn't support to check
// * Doesn't support column definition.
// * For column format:
//   * doesn't support comment.
//   * doesn't support column format, collate, storage.
//   * doesn't support reference.

func (parser *Parser) resolveCreateStm() (stm ast.Stm, err error) {
	if !parser.matchTokenTypes(false, lexer.CREATE) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch token.Tp {
	case lexer.TABLE:
		stm, err = parser.parseCreateTableStm()
	case lexer.DATABASE, lexer.SCHEMA:
		stm, err = parser.parseCreateDatabaseStm()
	default:
		err = parser.MakeSyntaxError(1, parser.pos)
	}
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(true, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	return stm, nil
}

// * create table [if not exist] tb_name like orig_tab_name;
// * create table [if not exist] tb_name2 (Column_Def..., Index_Def..., Constraint_Def...) [engine=value] [[Default | character set = value] | [Default | collate = value]];
// * create table [if not exist] as selectStatement;
func (parser *Parser) parseCreateTableStm() (stm ast.Stm, err error) {
	ifNotExist := parser.matchTokenTypes(true, lexer.IF, lexer.NOT, lexer.EXIST)
	tableName, ret := parser.parseIdentOrWord(true)
	if !ret || isTableNameEmpty(tableName) {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch token.Tp {
	case lexer.LIKE:
		stm, err = parser.parseCreateTableLikeStm(ifNotExist, tableName)
	case lexer.AS:
		stm, err = parser.parseCreateTableAsStm(ifNotExist, tableName)
	case lexer.LEFTBRACKET:
		stm, err = parser.parseClassicCreateTableStm(ifNotExist, tableName)
	default:
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return stm, err
}

func isTableNameEmpty(tableName []byte) bool {
	return len(tableName) == 0
}

// * create table [if not exist] tb_name like orig_tab_name;
func (parser *Parser) parseCreateTableLikeStm(ifNotExist bool, tableName []byte) (ast.Stm, error) {
	origTableName, ret := parser.parseIdentOrWord(false)
	if !ret || isTableNameEmpty(origTableName) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.CreateTableLikeStm{
		TableName:      string(tableName),
		IfNotExist:     ifNotExist,
		LikedTableName: string(origTableName),
	}, nil
}

// * create table [if not exist] tb_name2 (Column_Def..., Index_Def..., Constraint_Def...) [engine=value] [[Default | character set = value] | [Default | collate = value]];
func (parser *Parser) parseClassicCreateTableStm(ifNotExist bool, tableName []byte) (ast.Stm, error) {
	stm := &ast.CreateTableStm{
		TableName:  string(tableName),
		IfNotExist: ifNotExist,
	}
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var constraints []ast.Stm
	var columns []ast.ColumnDefStm
	var indexes []ast.IndexDefStm
	var col ast.ColumnDefStm
	var index ast.IndexDefStm
	var constraint ast.Stm
	var err error
	for {
		token, ok := parser.NextToken()
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		switch token.Tp {
		case lexer.WORD, lexer.IDENT:
			parser.UnReadToken()
			col, err = parser.parseColumnDef()
		case lexer.INDEX, lexer.KEY:
			parser.UnReadToken()
			index, err = parser.parseIndexDef()
		case lexer.CONSTRAINT, lexer.FOREIGN:
			parser.UnReadToken()
			constraint, err = parser.parseConstraintDef()
		default:
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
		if err != nil {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
		constraints = append(constraints, constraint)
		columns = append(columns, col)
		indexes = append(indexes, index)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	stm.Cols, stm.Indexes, stm.Constraints = columns, indexes, constraints
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if parser.matchTokenTypes(true, lexer.ENGINE, lexer.EQUAL) {
		engine, ok := parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		stm.Engine = string(engine)
	}
	charset, collate, ok := parser.parseCharsetAndCollate()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	stm.Charset, stm.Collate = charset, collate
	return stm, nil
}

// * create table [if not exist] as selectStatement;
func (parser *Parser) parseCreateTableAsStm(ifNotExist bool, tableName []byte) (ast.Stm, error) {
	selectStm, err := parser.resolveSelectStm(false)
	if err != nil {
		return nil, err
	}
	return ast.CreateTableAsSelectStm{
		TableName:  string(tableName),
		IfNotExist: ifNotExist,
		Select:     selectStm,
	}, nil
}

// For create database statement, if supports:
// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];
func (parser *Parser) parseCreateDatabaseStm() (ast.Stm, error) {
	ifNotExist := parser.matchTokenTypes(true, lexer.IF, lexer.NOT, lexer.EXIST)
	databaseName, ret := parser.parseIdentOrWord(false)
	if !ret || isTableNameEmpty(databaseName) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	stm := ast.NewCreateDatabaseStm(string(databaseName), ifNotExist)
	charset, collate, ok := parser.parseCharsetAndCollate()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	stm.Charset, stm.Collate = charset, collate
	return stm, nil
}

func (parser *Parser) parseCharsetAndCollate() (charset, collate string, ok bool) {
	if parser.matchTokenTypes(true, lexer.DEFAULT) {
		charset = "default"
	} else if parser.matchTokenTypes(true, lexer.CHARACTER, lexer.SET, lexer.EQUAL) {
		value, ret := parser.parseValue(false)
		if !ret {
			return
		}
		charset = string(value)
	}
	if parser.matchTokenTypes(true, lexer.DEFAULT) {
		collate = "default"
	} else if parser.matchTokenTypes(true, lexer.COLLATE, lexer.EQUAL) {
		value, ret := parser.parseValue(false)
		if !ret {
			return
		}
		collate = string(value)
	}
	return charset, collate, true
}
