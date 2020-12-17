package parser

// Rename statement can be rename table statement.
// It's like:
// * rename table {tb1 To tb2...}
func (parser *Parser) resolveRenameStm() (Stm, error) {
	if !parser.matchTokenTypes(false, RENAME) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	isTable := parser.matchTokenTypes(false, TABLE)
	if !isTable {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var origNames, modifiedNames []string
	for {
		origName, ret := parser.parseIdentOrWord(false)
		if !ret {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		if !parser.matchTokenTypes(false, TO) {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		modifiedName, ret := parser.parseIdentOrWord(false)
		if !ret {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		origNames = append(origNames, string(origName))
		modifiedNames = append(modifiedNames, string(modifiedName))
		if parser.matchTokenTypes(true, SEMICOLON) {
			break
		}
		if !parser.matchTokenTypes(false, COMMA) {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
	}
	return &RenameStm{OrigNames: origNames, ModifiedNames: modifiedNames}, nil
}
