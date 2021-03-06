package parser

// A index_def statement is like:
// {index|key} [index_name] (col_name, ...)

// Diff with mysql:
// * doesn't support index option and index type, cannot specify col length.
func (parser *Parser) parseIndexDef() (*IndexDefStm, error) {
	if !parser.matchTokenTypes(true, INDEX) && !parser.matchTokenTypes(true, KEY) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	indexName, _ := parser.parseIdentOrWord(true)
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	return &IndexDefStm{IndexName: string(indexName), ColNames: colNames}, nil
}
