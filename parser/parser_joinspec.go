package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// using (col,...)
func (parser *Parser) parseUsingJoinSpec() (ast.JoinSpecification, error) {
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyJoinSepc, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var cols []string
	for {
		col, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyJoinSepc, parser.MakeSyntaxError(1, parser.pos-1)
		}
		cols = append(cols, string(col))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyJoinSepc, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.JoinSpecification{Tp: ast.JoinSpecificationUsing, Condition: cols}, nil
}

// on whereStm
func (parser *Parser) parseOnJoinSpec() (ast.JoinSpecification, error) {
	if !parser.matchTokenTypes(false, lexer.ON) {
		return emptyJoinSepc, parser.MakeSyntaxError(1, parser.pos-1)
	}
	whereStm, err := parser.resolveWhereStm()
	if err != nil {
		return emptyJoinSepc, err
	}
	return ast.JoinSpecification{Tp: ast.JoinSpecificationON, Condition: whereStm}, nil
}
