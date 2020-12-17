package parser

// Insert statement is like:
// * insert into tb_name [( col_name... )] values (expression...)

func (parser *Parser) resolveInsertStm() (Stm, error) {
	if !parser.matchTokenTypes(false, INSERT) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, INTO) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	// () is optional
	var colNames []string
	if parser.matchTokenTypes(true, LEFTBRACKET) {
		for {
			colName, ret := parser.parseIdentOrWord(false)
			if !ret {
				return nil, parser.MakeSyntaxError(1, parser.pos-1)
			}
			colNames = append(colNames, string(colName))
			if !parser.matchTokenTypes(true, COMMA) {
				break
			}
		}
		// should be a )
		if !parser.matchTokenTypes(true, RIGHTBRACKET) {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
	}
	// should be values (
	if !parser.matchTokenTypes(false, VALUES, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var valueExpressions []*ExpressionStm
	for {
		valueExpression, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		valueExpressions = append(valueExpressions, valueExpression)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET, SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &InsertIntoStm{TableName: string(tableName), Cols: colNames, Values: valueExpressions}, nil
}
