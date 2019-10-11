package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const (
	WrongInsertFormatErr = ParseError("wrong insert format")
)

func (parser *Parser) resolveInsertStm() (*ast.InsertIntoStm, error) {
	// insert into ident|word [( ident|word[, ident|word])] values ( expression1[, expression1...] );
	if !parser.matchTokenType(lexer.INTO, false) {
		return nil, WrongInsertFormatErr
	}
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongInsertFormatErr
	}
	// () is optional
	var colNames []string
	if parser.matchTokenType(lexer.LEFTBRACKET, true) {
		for {
			colName, ret := parser.parseIdentOrWord(false)
			if !ret {
				return nil, WrongInsertFormatErr
			}
			colNames = append(colNames, colName)
			if parser.matchTokenType(lexer.COMMA, true) {
				continue
			}
			// should be a )
			if !parser.matchTokenType(lexer.RIGHTBRACKET, false) {
				return nil, WrongInsertFormatErr
			}
			break
		}
	}
	// should be values (
	if !parser.matchTokenTypes(false, lexer.VALUES, lexer.LEFTBRACKET) {
		return nil, WrongInsertFormatErr
	}
	var valueExpressions []ast.Stm
	for {
		valueExpression, err := parser.resolveExpression(true)
		if err != nil {
			return nil, err
		}
		valueExpressions = append(valueExpressions, valueExpression)
		if !parser.matchTokenType(lexer.COMMA, true) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET, lexer.SEMICOLON) {
		return nil, WrongInsertFormatErr
	}
	return &ast.InsertIntoStm{TableName: tableName, Cols: colNames, ValueExpressions: valueExpressions}, nil
}
