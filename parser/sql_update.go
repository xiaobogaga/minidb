package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const WrongUpdateStmErr = ParseError("wrong update statement err")

func (parser *Parser) resolveUpdateStm() (*ast.UpdateStm, error) {
	// update ident|word set expression[,expression]+ [WhereStm];
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret { return nil, WrongUpdateStmErr }
	if !parser.matchTokenType(lexer.SET, false) { return nil, WrongUpdateStmErr }
	var expressions []ast.Stm
	for {
		expression, err := parser.resolveExpression(false)
		if err != nil { return nil, WrongUpdateStmErr.Wrapper(err) }
		expressions = append(expressions, expression)
		if !parser.matchTokenType(lexer.COMMA, true) {
			break
		}
	}
	whereStm, err := parser.resolveWhereStm(true)
	if err != nil { return nil, WrongUpdateStmErr.Wrapper(err) }
	if !parser.matchTokenType(lexer.SEMICOLON, false) { return nil, WrongUpdateStmErr }
	return &ast.UpdateStm{TableName: tableName, Expressions: expressions, WhereStm: whereStm}, nil
}
