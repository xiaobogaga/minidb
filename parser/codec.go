package parser

import (
	"simpleDb/lexer"
	"strconv"
)

func DecodeValue(data []byte, tp lexer.TokenType) (interface{}, bool) {
	// Todo:
	// A value to
	switch tp {
	case lexer.BOOL:
		if len(data) == 0 {
			return false, false
		}
		if data[0] == '\'' || data[0] == '"' {
		}
		value, err := strconv.ParseInt(string(data), 10, 64)
		return value, err == nil
	case lexer.INT:
		if len(data) == 0 {
			return -1, false
		}
		if data[0] == '\'' || data[0] == '"' {
			value, err := strconv.ParseInt(string(data[1:len(data)-1]), 10, 64)
			return value, err == nil
		}
		value, err := strconv.ParseInt(string(data), 10, 64)
		return value, err == nil
	}
	return nil, false
}
