package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
	"strconv"
)

const WrongDeleteStmErr = ParseError("wrong delete statement err")
const WrongIntValueFormat = ParseError("wrong int value format")

func (parser *Parser) resolveDeleteStm() (stm *ast.DeleteStm, err error) {
	// Delete from ident|word  WhereStm OrderByStm LimitStm;
	if !parser.matchTokenType(lexer.FROM, false) {
		return nil, WrongDeleteStmErr
	}
	tableName, ret:= parser.parseIdentOrWord(false)
	if !ret {
		return nil, WrongDeleteStmErr
	}
	whereStm, err := parser.resolveWhereStm(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	orderByStm, err := parser.resolveOrderBy(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	limitStm, err := parser.resolveLimit(true)
	if err != nil {
		return nil, WrongDeleteStmErr.Wrapper(err)
	}
	// must end with ;
	if !parser.matchTokenType(lexer.SEMICOLON, false) {
		return nil, WrongDeleteStmErr
	}
	return ast.NewDeleteStm(tableName, whereStm, orderByStm, limitStm), nil
}

func (parser *Parser) parseIntValue(ifNotRollback bool) (int, error) {
	if !parser.hasNext() {
		return -1, TokensEndErr
	}
	t := parser.l.Tokens[parser.pos]
	if t.Tp != lexer.INTVALUE {
		if ifNotRollback { parser.pos -- }
		return -1, WrongIntValueFormat
	}
	v, err := strconv.Atoi(string(parser.l.Data[t.StartPos : t.EndPos]))
	if err != nil {
		if ifNotRollback { parser.pos -- }
		return -1, WrongIntValueFormat
	}
	return v, nil
}

