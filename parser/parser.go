package parser

import (
	"fmt"
	"simpleDb/ast"
	"simpleDb/lexer"
	"simpleDb/log"
	"strconv"
)

type Parser struct {
	pos int
	l   *lexer.Lexer
}

func NewParser() *Parser {
	return &Parser{}
}

type ParseError string

func (e ParseError) Error() string {
	return string(e)
}

func (e ParseError) Wrapper(err error) ParseError {
	return ParseError(fmt.Sprintf("%s: %s", e, err))
}

const (
	WrongSqlFormatErr = ParseError("wrong sql format err")
	FloatFormatErr    = ParseError("wrong float format")
	VarcharFormatErr  = ParseError("wrong varchar format")
)

func (parser *Parser) hasNext() bool {
	parser.pos++
	return parser.pos < len(parser.l.Tokens)
}

func (parser *Parser) getToken() lexer.Token {
	return parser.l.Tokens[parser.pos]
}

func (parser *Parser) parseIdentOrWord(ifNotRollback bool) (s string, ret bool) {
	if !parser.hasNext() {
		return "", false
	}
	t := parser.getToken()
	if t.Tp != lexer.IDENT && t.Tp != lexer.WORD {
		if ifNotRollback {
			parser.pos--
		}
		return "", false
	}
	if t.Tp == lexer.IDENT {
		return string(parser.l.Data[t.StartPos:t.EndPos]), true
	} else {
		return string(parser.l.Data[t.StartPos:t.EndPos]), true
	}
}

const WrongColumnValueFormatErr = ParseError("unknown value format")

func (parser *Parser) parseValue(ifNotRollback bool) (ast.ColumnValue, error) {
	if !parser.hasNext() {
		return ast.EmptyColumnValue, WrongColumnValueFormatErr
	}
	t := parser.getToken()
	switch t.Tp {
	case lexer.TRUE:
		return ast.NewColumnValue(lexer.TRUE, true), nil
	case lexer.FALSE:
		return ast.NewColumnValue(lexer.FALSE, false), nil
	case lexer.INTVALUE:
		v, err := strconv.Atoi(string(parser.l.Data[t.StartPos:t.EndPos]))
		if err != nil {
			return ast.EmptyColumnValue, WrongColumnValueFormatErr.Wrapper(err)
		}
		return ast.NewColumnValue(lexer.INTVALUE, v), nil
	case lexer.FLOATVALUE:
		v, err := strconv.ParseFloat(string(parser.l.Data[t.StartPos:t.EndPos]), t.EndPos-t.StartPos-1)
		if err != nil {
			return ast.EmptyColumnValue, err
		}
		return ast.NewColumnValue(lexer.FLOATVALUE, v), nil
	case lexer.CHARVALUE:
		return ast.NewColumnValue(lexer.CHARVALUE, parser.l.Data[t.StartPos]), nil
	case lexer.STRINGVALUE:
		return ast.NewColumnValue(lexer.STRINGVALUE, string(parser.l.Data[t.StartPos:t.EndPos])), nil
	}
	return ast.EmptyColumnValue, WrongColumnValueFormatErr
}

func (parser *Parser) Parse(data []byte) (ast.SqlStms, error) {
	lex := lexer.NewLexer()
	err := lex.Lex(data)
	if err != nil {
		return ast.EmptySqlStms, err
	}
	log.LogDebug("%s\n", lex)
	sqlStm := ast.SqlStms{}
	var stm ast.Stm
	parser.l = lex
	parser.pos = 0
	for ; parser.pos < len(parser.l.Tokens); parser.pos++ {
		token := parser.l.Tokens[parser.pos]
		switch token.Tp {
		case lexer.CREATE:
			stm, err = parser.resolveCreateStm()
		case lexer.DROP:
			stm, err = parser.resolveDropStm()
		case lexer.INSERT:
			stm, err = parser.resolveInsertStm()
		case lexer.DELETE:
			stm, err = parser.resolveDeleteStm()
		case lexer.UPDATE:
			stm, err = parser.resolveUpdateStm()
		case lexer.SELECT:
			stm, err = parser.resolveSelectStm()
		case lexer.TRUNCATE:
			stm, err = parser.resolveTruncate()
		case lexer.RENAME:
			stm, err = parser.resolveRename()
		case lexer.ALTER:
			stm, err = parser.resolveAlterStm()
		default:
			err = WrongSqlFormatErr
		}
		if err != nil {
			return ast.EmptySqlStms, err
		}
		sqlStm.Stms = append(sqlStm.Stms, stm)
	}
	return sqlStm, nil
}

func (parser *Parser) matchTokenType(tokenTp lexer.TokenType, ifNotRollback bool) bool {
	if !parser.hasNext() {
		return false
	}
	t := parser.getToken()
	if t.Tp != tokenTp {
		if ifNotRollback {
			parser.pos--
		}
		return false
	}
	return true
}

func (parser *Parser) matchTokenTypes(ifNotRollback bool, tokenTypes ...lexer.TokenType) bool {
	for i, tp := range tokenTypes {
		if !parser.hasNext() {
			return false
		}
		t := parser.getToken()
		if t.Tp != tp {
			if ifNotRollback {
				parser.pos -= i + 1
			}
			return false
		}
	}
	return true
}

func (parser *Parser) parseColumnDef() (col *ast.ColumnDefStm, err error) {
	// ident|word columnType [DEFAULT VALUE] [PRIMARY KEY]
	columnName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, ParseColumnErr
	}
	col = ast.NewColumnStm(columnName)
	col.ColumnType, err = parser.matchColumnType(false)
	if err != nil {
		return nil, ParseColumnErr.Wrapper(err)
	}
	if parser.matchTokenType(lexer.DEFAULT, true) {
		colValue, err := parser.parseValue(false)
		if err != nil {
			return nil, ParseColumnErr.Wrapper(err)
		}
		col.ColValue = colValue
	}
	col.PrimaryKey = parser.matchTokenTypes(true, lexer.PRIMARY, lexer.KEY)
	return col, err
}

func (parser *Parser) matchColumnType(ifNotRollback bool) (c ast.ColumnType, err error) {
	if !parser.hasNext() {
		err = TokensEndErr
		return
	}
	t := parser.getToken()
	switch t.Tp {
	case lexer.BOOL:
		return ast.NewColumnType(lexer.BOOL, 0, 0), nil
	case lexer.INT:
		return ast.NewColumnType(lexer.INT, 0, 0), nil
	case lexer.FLOAT:
		return parser.matchFloatType()
	case lexer.CHAR:
		return ast.NewColumnType(lexer.CHAR, 0, 0), nil
	case lexer.VARCHAR:
		return parser.matchVarcharType()
	case lexer.STRING:
		return ast.NewColumnType(lexer.STRING, 0, 0), nil
	}
	if ifNotRollback {
		parser.pos--
	}
	err = UnKnownTypeErr
	return
}

func (parser *Parser) matchFloatType() (c ast.ColumnType, err error) {
	// Float | Float ( INTVALUE, INTVALUE)
	if parser.matchTokenType(lexer.LEFTBRACKET, true) {
		if !parser.hasNext() {
			err = FloatFormatErr
			return
		}
		intValue1 := 0
		t := parser.l.Tokens[parser.pos]
		if t.Tp != lexer.INTVALUE {
			err = FloatFormatErr
			return
		}
		intValue1, err = strconv.Atoi(string(parser.l.Data[t.StartPos:t.EndPos]))
		if err != nil || !parser.matchTokenType(lexer.COMMA, false) || !parser.hasNext() {
			err = FloatFormatErr
			return
		}
		intValue2 := 0
		t = parser.l.Tokens[parser.pos]
		if t.Tp != lexer.INTVALUE {
			err = FloatFormatErr
			return
		}
		intValue2, err = strconv.Atoi(string(parser.l.Data[t.StartPos:t.EndPos]))
		if err != nil || !parser.matchTokenType(lexer.RIGHTBRACKET, false) {
			err = FloatFormatErr
			return
		}
		return ast.NewColumnType(lexer.FLOAT, intValue1, intValue2), nil
	}
	return ast.NewColumnType(lexer.FLOAT, -1, -1), nil
}

func (parser *Parser) matchVarcharType() (c ast.ColumnType, err error) {
	// VARCHAR ( INTVALUE )
	if !parser.matchTokenType(lexer.LEFTBRACKET, false) || !parser.hasNext() {
		err = VarcharFormatErr
		return
	}
	intValue := 0
	t := parser.l.Tokens[parser.pos]
	if t.Tp != lexer.INTVALUE {
		err = VarcharFormatErr
		return
	}
	intValue, err = strconv.Atoi(string(parser.l.Data[t.StartPos:t.EndPos]))
	if err != nil || !parser.matchTokenType(lexer.RIGHTBRACKET, false) {
		err = VarcharFormatErr
		return
	}
	return ast.NewColumnType(lexer.VARCHAR, intValue, 0), nil
}
