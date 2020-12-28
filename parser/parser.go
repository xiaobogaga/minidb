package parser

import (
	"fmt"
	"minidb/util"
	"strconv"
)

var parserLog = util.GetLog("Parser")

type Parser struct {
	pos    int
	Tokens []Token
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
	return fmt.Sprintf("syntax err near '%s'", s.ErrData)
}

func (parser *Parser) MakeSyntaxError(lineNumber, startPos int) error {
	// Todo, make sure whether lineNumber can be 1.
	return SyntaxError{
		LineNumber: lineNumber,
		ErrData:    string(parser.Data[parser.Tokens[startPos].StartPos:]),
	}
}

func (parser *Parser) Parse(data []byte) (stms []Stm, err error) {
	lex := NewLexer()
	tokens, err := lex.Lex(data)
	if err != nil {
		return nil, err
	}
	parser.Tokens = tokens
	parser.Data = data
	parser.pos = 0
	var stm Stm
	for {
		token, ok := parser.NextToken()
		if !ok {
			break
		}
		switch token.Tp {
		case CREATE:
			parser.UnReadToken()
			stm, err = parser.resolveCreateStm()
		case DROP:
			parser.UnReadToken()
			stm, err = parser.parseDropStm()
		case RENAME:
			parser.UnReadToken()
			stm, err = parser.resolveRenameStm()
		case ALTER:
			parser.UnReadToken()
			stm, err = parser.resolveAlterStm()
		case TRUNCATE:
			parser.UnReadToken()
			stm, err = parser.resolveTruncate()
		case INSERT:
			parser.UnReadToken()
			stm, err = parser.resolveInsertStm()
		case DELETE:
			parser.UnReadToken()
			stm, err = parser.resolveDeleteStm()
		case UPDATE:
			parser.UnReadToken()
			stm, err = parser.resolveUpdateStm()
		case SELECT:
			parser.UnReadToken()
			stm, err = parser.resolveSelectStm(true)
		case USE:
			parser.UnReadToken()
			stm, err = parser.resolveUseStm()
		case SHOW:
			parser.UnReadToken()
			stm, err = parser.resolveShowStm()
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

func (parser *Parser) NextToken() (Token, bool) {
	if parser.pos < len(parser.Tokens) {
		token := parser.Tokens[parser.pos]
		parser.pos++
		return token, true
	}
	parser.pos++
	return Token{}, false
}

func (parser *Parser) UnReadToken() {
	parser.pos--
}

func (parser *Parser) matchTokenTypes(ifNotRollback bool, tokenTypes ...TokenType) bool {
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

var emptyColumnTp = ColumnType{}

func (parser *Parser) parseColumnType(ifNotRollback bool) (ColumnType, bool) {
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
	case INT:
		ranges, success = parser.parseTypeRanges(true, 1)
	case BIGINT:
		ranges, success = parser.parseTypeRanges(true, 1)
	case FLOAT:
		ranges, success = parser.parseTypeRanges(true, 2)
	case CHAR:
		ranges, success = parser.parseTypeRanges(true, 1)
	case VARCHAR:
		ranges, success = parser.parseTypeRanges(true, 1)
	case BOOL, DATETIME, BLOB, MEDIUMBLOB, TEXT, MEDIUMTEXT:
		success = true
	default:
	}
	if !success {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return emptyColumnTp, false
	}
	return ColumnType{Tp: t.Tp, Ranges: ranges}, true
}

func DecodeInt(data []byte) (int, bool) {
	value, err := strconv.ParseInt(string(data), 10, 64)
	return int(value), err == nil
}

var emptyRange = [2]int{0, 0}

// parseTypeRanges try to parse a range from a type def, such as (5) of int(5), (10, 2) of float(10, 2).
func (parser *Parser) parseTypeRanges(ifNotRollback bool, rangeSize int) (ret [2]int, success bool) {
	if !parser.matchTokenTypes(true, LEFTBRACKET) {
		return emptyRange, true
	}
	for i := 0; i < rangeSize; i++ {
		if i != 0 && !parser.matchTokenTypes(true, COMMA) {
			break
		}
		value, ok := parser.parseValue(false)
		if !ok {
			if ifNotRollback {
				parser.pos -= i + 2
			}
			return
		}
		r, success := DecodeInt(value)
		if !success {
			if ifNotRollback {
				parser.pos -= i + 2
			}
			return ret, success
		}
		ret[i] = r
	}
	if !parser.matchTokenTypes(true, RIGHTBRACKET) {
		parser.pos -= rangeSize + 1
		return
	}
	return ret, true
}

func (parser *Parser) parseIdentOrWord(ifNotRollback bool) (s []byte, ret bool) {
	t, ok := parser.NextToken()
	if !ok || (t.Tp != IDENT && t.Tp != WORD) {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return nil, false
	}
	if t.Tp == IDENT {
		return parser.Data[t.StartPos:t.EndPos], true
	} else {
		return parser.Data[t.StartPos:t.EndPos], true
	}
}

func (parser *Parser) parseValue(ifNotRollback bool) ([]byte, bool) {
	t, ok := parser.NextToken()
	if !ok || t.Tp != VALUE {
		if ifNotRollback {
			parser.UnReadToken()
		}
		return nil, false
	}
	return parser.Data[t.StartPos:t.EndPos], true
}
