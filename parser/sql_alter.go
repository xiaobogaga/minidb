package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongAlterFormatErr = ParseError("wrong alter table format err")

func (parser *Parser) resolveAlterStm() (stm *ast.AlterStm, err error) {
	// ALTER [TABLE] TB_NAME ([ADD [COLUMN] COLDEF) | ([DROP [COLUMN] COL_NAME]) | (ALTER [COLUMN) COLDEF) |
	// (CHANGE [COLUMN] OLD_COLUMN COLDEF)
	parser.matchTokenType(lexer.TABLE, true)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongAlterFormatErr
	}
	if !parser.hasNext() {
		return nil, WrongAlterFormatErr
	}
	var colDef *ast.ColumnDefStm
	t := parser.getToken()
	switch t.Tp {
	case lexer.COLADD:
		colDef, err = parser.parseAddCol()
	case lexer.DROP:
		colDef, err = parser.parseDropCol()
	case lexer.ALTER:
		colDef, err = parser.parseAlterCol()
	case lexer.CHANGE:
		colDef, err = parser.parseChangeCol()
	default:
		return nil, WrongAlterFormatErr
	}
	if err != nil {
		return nil, WrongAlterFormatErr.Wrapper(err)
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, WrongAlterFormatErr
	}
	return &ast.AlterStm{Tp: t.Tp, TableName: tableName, ColDef: colDef}, nil
}

func (parser *Parser) parseAddCol() (*ast.ColumnDefStm, error) {
	parser.matchTokenType(lexer.COLUMN, true)
	return parser.parseColumnDef()
}

func (parser *Parser) parseDropCol() (*ast.ColumnDefStm, error) {
	parser.matchTokenType(lexer.COLUMN, true)
	colName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongAlterFormatErr
	}
	return ast.NewColumnStm(colName), nil
}

func (parser *Parser) parseAlterCol() (*ast.ColumnDefStm, error) {
	parser.matchTokenType(lexer.COLUMN, true)
	return parser.parseColumnDef()
}

func (parser *Parser) parseChangeCol() (*ast.ColumnDefStm, error) {
	parser.matchTokenType(lexer.COLUMN, true)
	oldColName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongAlterFormatErr
	}
	colDef, err := parser.parseColumnDef()
	if err != nil {
		return nil, err
	}
	colDef.OldColName = oldColName
	return colDef, nil
}