package parser

// Limit statement is like limit {[offset,] row_counter | row_counter OFFSET offset}

func (parser *Parser) parseLimit() (*LimitStm, error) {
	if !parser.matchTokenTypes(true, LIMIT) {
		return nil, nil
	}
	ret, ok := parser.parseValue(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	value, ok := DecodeInt(ret)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if parser.matchTokenTypes(true, COMMA) {
		ret, ok = parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		rowCounter, ok := DecodeInt(ret)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		return &LimitStm{Offset: value, Count: rowCounter}, nil
	}
	if parser.matchTokenTypes(true, OFFSET) {
		ret, ok := parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		offset, ok := DecodeInt(ret)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		return &LimitStm{Count: value, Offset: offset}, nil
	}
	return &LimitStm{Count: value}, nil
}
