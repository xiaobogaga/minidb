package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Insert statement is like:
// * insert into tb_name [( col_name... )] values (expression...)

func (parser *Parser) resolveInsertStm() (*ast.InsertIntoStm, error) {
	if !parser.matchTokenTypes(false, lexer.INSERT) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.INTO) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	// () is optional
	var colNames []string
	if parser.matchTokenTypes(true, lexer.LEFTBRACKET) {
		for {
			colName, ret := parser.parseIdentOrWord(false)
			if !ret {
				return nil, parser.MakeSyntaxError(1, parser.pos-1)
			}
			colNames = append(colNames, string(colName))
			if !parser.matchTokenTypes(true, lexer.COMMA) {
				break
			}
		}
		// should be a )
		if !parser.matchTokenTypes(true, lexer.RIGHTBRACKET) {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
	}
	// should be values (
	if !parser.matchTokenTypes(false, lexer.VALUES, lexer.LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var valueExpressions []*ast.ExpressionStm
	for {
		valueExpression, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		valueExpressions = append(valueExpressions, valueExpression)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.InsertIntoStm{TableName: string(tableName), Cols: colNames, Values: valueExpressions}, nil
}
