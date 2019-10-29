package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var columnDefStm = ast.ColumnDefStm{}

// A columnDef statement is:
// col_name col_type [not null|null] [default default_value] [AUTO_INCREMENT] [unique [key]] [[primary] key]

// parseColumnDef parse a column definition statement and return it.

func (parser *Parser) parseColumnDef() (ast.ColumnDefStm, error) {
	columnName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return columnDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	col := ast.NewColumnStm(string(columnName))
	colType, success := parser.parseColumnType(false)
	if !success {
		return columnDefStm, parser.MakeSyntaxError(1, parser.pos)
	}
	col.ColumnType = colType
	if parser.matchTokenTypes(true, lexer.NULL) {
		col.AllowNULL = true
	} else if parser.matchTokenTypes(true, lexer.NOT, lexer.NULL) {
		col.AllowNULL = false
	}
	if parser.matchTokenTypes(true, lexer.DEFAULT) {
		colValue, success := parser.parseValue(false)
		if !success {
			return columnDefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		col.ColDefaultValue = colValue
	}
	if parser.matchTokenTypes(true, lexer.AUTO_INCREMENT) {
		col.AutoIncrement = true
	}
	if parser.matchTokenTypes(true, lexer.UNIQUE) || parser.matchTokenTypes(true, lexer.UNIQUE, lexer.KEY) {
		col.UniqueKey = true
	}
	if parser.matchTokenTypes(true, lexer.PRIMARY) || parser.matchTokenTypes(true, lexer.PRIMARY, lexer.KEY) {
		col.PrimaryKey = true
	}
	return col, nil
}
