package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Truncate table statement is like:
// * truncate [table] tb_name

func (parser *Parser) resolveTruncate() (ast.Stm, error) {
	if !parser.matchTokenTypes(false, lexer.TRUNCATE) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	parser.matchTokenTypes(true, lexer.TABLE)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.TruncateStm{TableName: string(tableName)}, nil
}
