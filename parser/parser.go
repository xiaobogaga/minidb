package parser

import (
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/util"
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
	ErrData string
}

func (s SyntaxError) Error() string {
	return fmt.Sprintf("syntax err near '%s'", s.ErrData)
}

func (parser *Parser) MakeSyntaxError(startPos int) error {
	if startPos >= len(parser.Tokens) {
		startPos = len(parser.Tokens) - 1
	}
	return SyntaxError{
		ErrData: string(parser.Data[parser.Tokens[startPos].StartPos:]),
	}
}

func (parser *Parser) Parse(data []byte) (stm Stm, err error) {
	lex := NewLexer()
	tokens, err := lex.Lex(data)
	if err != nil {
		return nil, err
	}
	parser.Tokens = tokens
	parser.Data = data
	parser.pos = 0
	token, ok := parser.NextToken()
	if !ok {
		return nil, errors.New("syntax err: please input query")
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
		return nil, errors.New("syntax err: Alter is not supported now")
		// parser.UnReadToken()
		// stm, err = parser.resolveAlterStm()
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
	case BEGIN, COMMIT, ROLLBACK:
		parser.UnReadToken()
		stm, err = parser.ParseTransStm()
	default:
		err = parser.MakeSyntaxError(parser.pos - 1)
	}
	if err != nil {
		return nil, err
	}
	if parser.pos < len(parser.Tokens) {
		return nil, parser.MakeSyntaxError(parser.pos)
	}
	return
}

func (parser *Parser) Set(data []byte) error {
	lexer := NewLexer()
	tokens, err := lexer.Lex(data)
	if err != nil {
		return err
	}
	parser.Tokens = tokens
	parser.Data = data
	parser.pos = 0
	return nil
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

func (parser *Parser) parseColumnType() (ColumnType, error) {
	t, ok := parser.NextToken()
	if !ok {
		return emptyColumnTp, parser.MakeSyntaxError(parser.pos - 1)
	}
	var ranges [2]int
	var success bool
	switch t.Tp {
	case INT:
		ranges, success = parser.parseTypeRanges(true, 1, false)
	case BIGINT:
		ranges, success = parser.parseTypeRanges(true, 1, false)
	case FLOAT:
		ranges, success = parser.parseTypeRanges(true, 2, false)
	case CHAR:
		ranges, success = parser.parseTypeRanges(true, 1, false)
	case VARCHAR:
		ranges, success = parser.parseTypeRanges(true, 1, true)
	case BOOL, DATETIME, DATE, TIME, BLOB, MEDIUMBLOB, TEXT, MEDIUMTEXT:
		success = true
	default:
	}
	if !success {
		return emptyColumnTp, parser.MakeSyntaxError(parser.pos - 1)
	}
	// If range is empty, we need to set it to default range.
	ranges = getDefaultRanges(ranges, t.Tp)
	return ColumnType{Tp: t.Tp, Ranges: ranges}, nil
}

func getDefaultRanges(ranges [2]int, tp TokenType) [2]int {
	if ranges[0] != -1 {
		return ranges
	}
	switch tp {
	case FLOAT:
		ranges[0], ranges[1] = 10, 2
	case CHAR:
		ranges[0], ranges[1] = 1, 0
	}
	return ranges
}

func DecodeInt(data []byte) (int, bool) {
	value, err := strconv.ParseInt(string(data), 10, 64)
	return int(value), err == nil
}

var emptyRange = [2]int{-1, -1}

// parseTypeRanges try to parse a range from a type def, such as (5) of int(5), (10, 2) of float(10, 2).
func (parser *Parser) parseTypeRanges(ifNotRollback bool, rangeSize int, mandatory bool) (ret [2]int, success bool) {
	if !parser.matchTokenTypes(true, LEFTBRACKET) {
		return emptyRange, !mandatory
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
		if !success || r < 0 {
			if ifNotRollback {
				parser.pos -= i + 2
			}
			return ret, false
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
