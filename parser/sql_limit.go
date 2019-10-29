package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Limit statement is like limit {[offset,] row_counter | row_counter OFFSET offset}

func (parser *Parser) parseLimit() (*ast.LimitStm, error) {
	if !parser.matchTokenTypes(true, lexer.LIMIT) {
		return nil, nil
	}
	ret, ok := parser.parseValue(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	value, ok := DecodeValue(ret, lexer.INT)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	if parser.matchTokenTypes(true, lexer.COMMA) {
		ret, ok = parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		rowCounter, ok := DecodeValue(ret, lexer.INT)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
		return &ast.LimitStm{Offset: value.(int), Count: rowCounter.(int)}, nil
	}
	if parser.matchTokenTypes(true, lexer.OFFSET) {
		ret, ok := parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		offset, ok := DecodeValue(ret, lexer.INT)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos)
		}
		return &ast.LimitStm{Count: value.(int), Offset: offset.(int)}, nil
	}
	return &ast.LimitStm{Count: value.(int)}, nil
}
