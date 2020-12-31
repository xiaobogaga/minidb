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

func verifyTestPlan(t *testing.T, sql string) {
	p := parser.NewParser()
	stm, err := p.Parse([]byte(sql))
	assert.Nil(t, err)
	plan, err := MakeLogicPlan(stm.(*parser.SelectStm), "db1")
	assert.Nil(t, err)
	data, err := json.MarshalIndent(plan, "", "\t")
	assert.Nil(t, err)
	println(string(data))
}

func TestMakeProjectionScanLogicPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1;"
	verifyTestPlan(t, sql)
	sql = "select id, name from test1;"
	verifyTestPlan(t, sql)
	sql = "select id, name from test1 where id = 1;"
	verifyTestPlan(t, sql)
	sql = "select id, name from test1 where id = 1 * 2 + 3;"
	verifyTestPlan(t, sql)
	sql = "select name from test1 where name = 'hello';"
	verifyTestPlan(t, sql)
}

func TestMakeOrderByLogicPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1 order by id + id asc;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 order by id + id desc;"
	verifyTestPlan(t, sql)
}

func TestMakeJoinLogicPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1, test2;"
	verifyTestPlan(t, sql)
}

func TestMakeGroupByPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select id from test1 group by id;"
	verifyTestPlan(t, sql)
	sql = "select id, count(sum) from test1 group by id;"
	verifyTestPlan(t, sql)
	sql = "select id, count(sum) from test1 group by id, name;"
	verifyTestPlan(t, sql)
}

func TestMakeLimitLogicPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1 limit 5;"
	verifyTestPlan(t, sql)
}
