package parser

// Limit statement is like limit {[offset,] row_counter | row_counter OFFSET offset}

func (parser *Parser) parseLimit() (*LimitStm, error) {
	if !parser.matchTokenTypes(true, LIMIT) {
		return nil, nil
	}
	ret, ok := parser.parseValue(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	value, ok := DecodeValue(ret, INT)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	if parser.matchTokenTypes(true, COMMA) {
		ret, ok = parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		rowCounter, ok := DecodeValue(ret, INT)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
		return &LimitStm{Offset: value.(int), Count: rowCounter.(int)}, nil
	}
	if parser.matchTokenTypes(true, OFFSET) {
		ret, ok := parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		offset, ok := DecodeValue(ret, INT)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
		return &LimitStm{Count: value.(int), Offset: offset.(int)}, nil
	}
	return &LimitStm{Count: value.(int)}, nil
}
