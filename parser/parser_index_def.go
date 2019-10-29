package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var emptyIndexDefStm = ast.IndexDefStm{}

// A index_def statement is like:
// {index|key} [index_name] (col_name, ...)

// Diff with mysql:
// * doesn't support index option and index type, cannot specify col length.
func (parser *Parser) parseIndexDef() (ast.IndexDefStm, error) {
	if !parser.matchTokenTypes(true, lexer.INDEX) && !parser.matchTokenTypes(true, lexer.KEY) {
		return emptyIndexDefStm, parser.MakeSyntaxError(1, parser.pos)
	}
	indexName, _ := parser.parseIdentOrWord(true)
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyIndexDefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	return ast.IndexDefStm{IndexName: string(indexName), ColNames: colNames}, nil
}
