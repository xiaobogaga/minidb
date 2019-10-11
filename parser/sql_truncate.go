package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongTruncateTableStm = ParseError("wrong truncate format")

func (parser *Parser) resolveTruncate() (*ast.TruncateStm, error) {
	// truncate [table] tableName
	parser.matchTokenType(lexer.TABLE, true)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongTruncateTableStm
	}
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, WrongTruncateTableStm
	}
	return &ast.TruncateStm{TableName: tableName}, nil
}
