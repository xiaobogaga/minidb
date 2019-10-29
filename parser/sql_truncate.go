package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var emptyTruncateStm = ast.TruncateStm{}

// Truncate table statement is like:
// * truncate [table] tb_name

func (parser *Parser) resolveTruncate() (ast.TruncateStm, error) {
	if !parser.matchTokenTypes(false, lexer.TRUNCATE) {
		return emptyTruncateStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	parser.matchTokenTypes(true, lexer.TABLE)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return emptyTruncateStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return emptyTruncateStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.TruncateStm{TableName: string(tableName)}, nil
}
