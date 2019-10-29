package parser

import (
	"fmt"
	"simpleDb/ast"
	"simpleDb/lexer"
	"simpleDb/log"
)

var parserLog = log.GetLog("Parser")

type Parser struct {
	pos    int
	Tokens []lexer.Token
	Data   []byte
}

func NewParser() *Parser {
	return &Parser{}
}

type SyntaxError struct {
	LineNumber int
	ErrData    string
}

func (s SyntaxError) Error() string {
	return fmt.Sprintf("syntax err near %s at %d line", s.ErrData, s.LineNumber)
}

func (parser *Parser) MakeSyntaxError(lineNumber, startPos int) error {
	// Todo, make sure whether lineNumber can be 1.
	return SyntaxError{
		LineNumber: lineNumber,
		ErrData:    string(parser.Data[parser.Tokens[startPos].StartPos:]),
	}
}

func (parser *Parser) Parse(data []byte) (stms []ast.Stm, err error) {
	lex := lexer.NewLexer()
	tokens, err := lex.Lex(data)
	if err != nil {
		return nil, err
	}
	parser.Tokens = tokens
	parser.Data = data
	parser.pos = -1
	var stm ast.Stm
	for {
		token, ok := parser.NextToken()
		if !ok {
			break
		}
		switch token.Tp {
		case lexer.CREATE:
			parser.UnReadToken()
			stm, err = parser.resolveCreateStm()
		case lexer.DROP:
			parser.UnReadToken()
			stm, err = parser.parseDropStm()
		case lexer.RENAME:
			parser.UnReadToken()
			stm, err = parser.resolveRenameStm()
		case lexer.ALTER:
			parser.UnReadToken()
			stm, err = parser.resolveAlterStm()
		case lexer.TRUNCATE:
			parser.UnReadToken()
			stm, err = parser.resolveTruncate()
		case lexer.INSERT:
			parser.UnReadToken()
			stm, err = parser.resolveInsertStm()
		case lexer.DELETE:
			parser.UnReadToken()
			stm, err = parser.resolveDeleteStm()
		case lexer.UPDATE:
			parser.UnReadToken()
			stm, err = parser.resolveUpdateStm()
		case lexer.SELECT:
			parser.UnReadToken()
			stm, err = parser.resolveSelectStm(true)
		default:
			err = parser.MakeSyntaxError(1, parser.pos)
		}
		if err != nil {
			return nil, err
		}
		stms = append(stms, stm)
	}
	return
}

func (parser *Parser) NextToken() (lexer.Token, bool) {
	if parser.pos < len(parser.Tokens) {
		token := parser.Tokens[parser.pos]
		parser.pos++
		return token, true
	}
	parser.pos++
	return lexer.Token{}, false
}

func (parser *Parser) UnReadToken() {
	parser.pos--
}

func (parser *Parser) matchTokenTypes(ifNotRollback bool, tokenTypes ...lexer.TokenType) bool {
	for i, tp := range tokenTypes {
		t, ok := parser.NextToken()
		if !ok || t.Tp != tp {
			if ifNotRollback {
				parser.pos -= i + 1
			}
			return false
		}
	}
	return true
}

var emptyColumnTp = ast.ColumnType{}

func (parser *Parser) parseColumnType(ifNotRollback bool) (ast.ColumnType, bool) {
	t, ok := parser.NextToken()
	if !ok {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return emptyColumnTp, false
	}
	var ranges [2]int
	var success bool
	switch t.Tp {
	case lexer.INT:
		ranges, success = parser.parseTypeRanges(true, 1)
	case lexer.BIGINT:
		ranges, success = parser.parseTypeRanges(true, 1)
	case lexer.FLOAT:
		ranges, success = parser.parseTypeRanges(true, 2)
	case lexer.CHAR:
		ranges, success = parser.parseTypeRanges(true, 1)
	case lexer.VARCHAR:
		ranges, success = parser.parseTypeRanges(true, 1)
	case lexer.BOOL, lexer.DATETIME, lexer.BLOB, lexer.MEDIUMBLOB, lexer.TEXT, lexer.MEDIUMTEXT:
	default:
	}
	if !success {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return emptyColumnTp, false
	}
	return ast.MakeColumnType(t.Tp, ranges), true
}

// parseTypeRanges try to parse a range from a type def, such as (5) of int(5), (10, 2) of float(10, 2).
func (parser *Parser) parseTypeRanges(ifNotRollback bool, rangeSize int) (ret [2]int, success bool) {
	if !parser.matchTokenTypes(true, lexer.LEFTBRACKET) {
		return
	}
	for i := 0; i < rangeSize; i++ {
		value, ok := parser.parseValue(false)
		if !ok {
			if ifNotRollback {
				parser.pos -= i + 2
			}
			return
		}
		r, success := DecodeValue(value, lexer.INT)
		if !success {
			if ifNotRollback {
				parser.pos -= i + 2
			}
			return
		}
		ret[i] = r.(int)
	}
	if !parser.matchTokenTypes(true, lexer.RIGHTBRACKET) {
		parser.pos -= rangeSize + 1
		return
	}
	return ret, true
}

func (parser *Parser) parseIdentOrWord(ifNotRollback bool) (s []byte, ret bool) {
	t, ok := parser.NextToken()
	if !ok || (t.Tp != lexer.IDENT && t.Tp != lexer.WORD) {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return nil, false
	}
	if t.Tp == lexer.IDENT {
		return parser.Data[t.StartPos+1 : t.EndPos-1], true
	} else {
		return parser.Data[t.StartPos:t.EndPos], true
	}
}

func (parser *Parser) parseValue(ifNotRollback bool) ([]byte, bool) {
	t, ok := parser.NextToken()
	if !ok || t.Tp != lexer.VALUE {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return nil, false
	}
	if parser.Data[t.StartPos] == '\'' || parser.Data[t.StartPos] == '"' {
		return parser.Data[t.StartPos+1 : t.EndPos-1], true
	}
	return parser.Data[t.StartPos:t.EndPos], true
}
