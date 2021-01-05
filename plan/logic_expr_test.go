package plan

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/xiaobogaga/parser"
	"testing"
)

func verifyTestExpr(t *testing.T, sql string) {
	parser := parser.NewParser()
	parser.Set([]byte(sql))
	stm, err := parser.ResolveWhereStm()
	assert.Nil(t, err)
	expr := ExprStmToLogicExpr(stm, nil)
	data, err := json.MarshalIndent(expr, "", "\t")
	assert.Nil(t, err)
	println(string(data))
}

func TestMakeLogicExpr(t *testing.T) {
	sql := "where id=1 * name + c % 1"
	verifyTestExpr(t, sql)
	sql = "where max(id, age, sum(name)) + max(id) + id * 5"
	verifyTestExpr(t, sql)
}

func TestLogicExpr_TypeCheck(t *testing.T) {
	initTestStorage(t)
	sql := "select * from db1.test1 where id = 1 + 0;"
	testSelect(t, sql, 1, false)
	sql = "select * from db1.test1 where id = 1 + 0 or id = 2;"
	testSelect(t, sql, 2, false)
	sql = "select * from db1.test1 where id = id > 0;"
	testSelect(t, sql, 0, true)
}
