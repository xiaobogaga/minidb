package parser

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testOneExpression(t *testing.T, data []byte) {
	parser := NewParser()
	lex := NewLexer()
	tokens, err := lex.Lex(data)
	assert.Nil(t, err)
	parser.Tokens = tokens
	parser.pos = 0
	parser.Data = data
	expr, err := parser.resolveExpression()
	assert.Nil(t, err)
	str, err := json.MarshalIndent(expr, "", "\t")
	assert.Nil(t, err)
	println(string(str))
}

func TestParser_Expression(t *testing.T) {
	sql := "(a + b) * c + min(a, b, sum(c, 10))"
	testOneExpression(t, []byte(sql))
	sql = "a > b and (c = 1) + d"
	testOneExpression(t, []byte(sql))
	sql = "id = 1 + 0 or id = 2"
	testOneExpression(t, []byte(sql))
	sql = " id = 1 * 1 + 1 or id = 1"
	testOneExpression(t, []byte(sql))
}
