package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var emptyRenameStm = ast.RenameStm{}

// Rename statement can be rename table statement.
// It's like:
// * rename table {tb1 To tb2...}
func (parser *Parser) resolveRenameStm() (ast.RenameStm, error) {
	if !parser.matchTokenTypes(false, lexer.RENAME) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	isTable := parser.matchTokenTypes(false, lexer.TABLE)
	if !isTable {
		return emptyRenameStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var origNames, modifiedNames []string
	for {
		origName, ret := parser.parseIdentOrWord(false)
		if !ret {
			return emptyRenameStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		if !parser.matchTokenTypes(false, lexer.TO) {
			return emptyRenameStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		modifiedName, ret := parser.parseIdentOrWord(false)
		if !ret {
			return emptyRenameStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		origNames = append(origNames, string(origName))
		modifiedNames = append(modifiedNames, string(modifiedName))
		if parser.matchTokenTypes(true, lexer.SEMICOLON) {
			break
		}
		if !parser.matchTokenTypes(false, lexer.COMMA) {
			return emptyRenameStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
	}
	return ast.RenameStm{OrigNames: origNames, ModifiedNames: modifiedNames}, nil
}
