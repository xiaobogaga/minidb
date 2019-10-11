package parser

import (
	"github.com/stretchr/testify/assert"
	"simpleDb/lexer"
	"simpleDb/log"
	"testing"
)

func TestWhereStm(t *testing.T) {
	sqls := []string{
		"where 5 + 2 + 3",
		"where (5 + 2 * 1) + (2 + 1 * Max(5 * 4))",
		"where Max(name + 10) + Min(age + count(col + 100))",
		"where name == 10 and 100 == age and (Max(c) == 100 and sum(min(age) + 10) != 10)",
	}
	parser := NewParser()
	lexer := lexer.NewLexer()
	for _, sql := range sqls {
		lexer.Reset()
		err := lexer.Lex([]byte(sql))
		assert.Nil(t, err, sql)
		parser.l = lexer
		parser.pos = -1
		stm, err := parser.resolveWhereStm(false)
		log.LogDebug("%v\n", stm)
		assert.Nil(t, err, sql)
	}
}

func TestFunctionCallStm(t *testing.T) {
	sqls := []string{
		"Max(5 + 2 + Min(2), 100)",
		"Min(Sum(name))",
		"Max(Min(name) + Sum(name) + Max(age + 10 + min(max(count))), sum(age) * min(10) + calc(sum(10)))",
	}
	parser := NewParser()
	lexer := lexer.NewLexer()
	for _, sql := range sqls {
		lexer.Reset()
		err := lexer.Lex([]byte(sql))
		assert.Nil(t, err, sql)
		parser.l = lexer
		parser.pos = -1
		stm, err := parser.resolveFunctionCall(false)
		log.LogDebug("%v\n", stm)
		assert.Nil(t, err, sql)
	}
}
