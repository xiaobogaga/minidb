package plan

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"minidb/parser"
	"testing"
)

var testDataSize = 4

func initTestStorage(t *testing.T) {
	parser := parser.NewParser()
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(id int primary key, name varchar(20), age float, location varchar(20));",
		"create table test2(id int primary key, name varchar(20), age float, location varchar(20));",
		"create database db2;",
		"use db2;",
		"create table test1(id int primary key, name varchar(20), age float, location varchar(20));",
		"create table test2(id int primary key, name varchar(20), age float, location varchar(20));",
	}
	currentDB := ""
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
	currentDB = "db1"
	// insert some data to db1 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1, '%d');", i, i, testDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1, '%d');", i, i, testDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err = parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err = MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
	currentDB = "db2"
	// insert some data to db2 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1, '%d');", i, i, testDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1, '%d');", i, i, testDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err = parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err = MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
}

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
	testSelect(t, sql)
	sql = "select * from db1.test1 where id = 1 + 0 or id = 2;"
	testSelect(t, sql)
}
