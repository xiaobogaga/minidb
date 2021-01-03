package plan

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"minidb/parser"
	"testing"
	"time"
)

var testDataSize = 4

func initTestStorage(t *testing.T) {
	parser := parser.NewParser()
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(id int primary key, name varchar(20), age float, location varchar(20), loc1 int);",
		"create table test2(id int primary key, name varchar(20), age float, location varchar(20), loc2 int);",
		"create database db2;",
		"use db2;",
		"create table test1(id int primary key, name varchar(20), age float, location varchar(20), loc1 int);",
		"create table test2(id int primary key, name varchar(20), age float, location varchar(20), loc2 int);",
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
	random := rand.New(rand.NewSource(time.Now().Unix()))
	currentDB = "db1"
	// insert some data to db1 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d.%d', %d.1, '%d', 0);", i, random.Int31n(1000), i, testDataSize-(i*int(random.Int31n(10))), i%2)
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d.%d', %d.1, '%d', 0);", i, random.Int31n(1000), i, testDataSize-(i*int(random.Int31n(10))), i%2)
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
		sql := fmt.Sprintf("insert into test1 values(%d, '%d.%d', %d.1, '%d', 0);", i, random.Int31n(1000), i, testDataSize-(i*int(random.Int31n(10))), i%2)
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d.%d', %d.1, '%d', 0);", i, random.Int31n(1000), i, testDataSize-(i*int(random.Int31n(10))), i%2)
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

func verifyTestPlanFail(t *testing.T, sql string) {
	p := parser.NewParser()
	stm, err := p.Parse([]byte(sql))
	if err != nil {
		return
	}
	_, err = MakeLogicPlan(stm.(*parser.SelectStm), "db1")
	assert.NotNil(t, err)
}

func TestMakeProjectionScanLogicPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1;"
	verifyTestPlan(t, sql)
	sql = "select id, name from test1;"
	verifyTestPlan(t, sql)
	sql = "select id, name from test1 where id = 1 or id = 2 and id = 3;"
	verifyTestPlan(t, sql)
	sql = "select id + 1, name from test1 where id = 1 * 2 + 3 / 2 % 1;"
	verifyTestPlan(t, sql)
	sql = "select name from test1 where name = 'hello';"
	verifyTestPlan(t, sql)
	sql = "select name from test1 where name > 'hello';"
	verifyTestPlan(t, sql)
	sql = "select name from test1 where name >= 'hello';"
	verifyTestPlan(t, sql)
	sql = "select name from test1 where name < 'hello';"
	verifyTestPlan(t, sql)
	sql = "select name from test1 where name <= 'hello';"
	verifyTestPlan(t, sql)
	sql = "select sum(id) from test1 where name != 'hello';"
	verifyTestPlan(t, sql)
	sql = "select ttt from test1 where name = 'hello';"
	verifyTestPlanFail(t, sql)
	sql = "select id, ttt from test1 where name = 'hello';"
	verifyTestPlanFail(t, sql)
	sql = "select id, name from test1 where ttt = 'hello';"
	verifyTestPlanFail(t, sql)
	sql = "select ttt(id) from test1 where name = 'hello';"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 where id = 'hello';"
	verifyTestPlanFail(t, sql)
}

func TestMakeOrderByLogicPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1 order by id + id asc;"
	verifyTestPlan(t, sql)
	sql = "select id + 1 from test1 order by id + id desc;"
	verifyTestPlan(t, sql)
	sql = "select id, sum(id) from test1 order by id desc;"
	verifyTestPlan(t, sql)
	sql = "select id, sum(id) from test1 order by id desc, name asc;"
	verifyTestPlan(t, sql)
	sql = "select sum(id) from test1 order by id;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 order by id desc, col asc;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 order by col asc, id asc;"
	verifyTestPlanFail(t, sql)
}

func TestMakeJoinLogicPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1, test2;"
	verifyTestPlan(t, sql)
	sql = "select * from test1 inner join test2;"
	verifyTestPlan(t, sql)
	sql = "select * from test1 join test2;"
	verifyTestPlan(t, sql)
	sql = "select test1.id from test1 left join test2 on test1.id = test2.id;"
	verifyTestPlan(t, sql)
	sql = "select test1.id from test1 left join test2;"
	verifyTestPlanFail(t, sql)
	sql = "select test1.id from test1 right join test2 on test1.id = test2.id;"
	verifyTestPlan(t, sql)
	sql = "select test1.id from test1 right join test2;"
	verifyTestPlanFail(t, sql)
	sql = "select test1.id from test1 right join test2 using (id);"
	verifyTestPlan(t, sql)
	sql = "select test1.id from test1 right join test2 using (col);"
	verifyTestPlanFail(t, sql)
	sql = "select test1.id from test1 right join test2 using (loc1);"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 left join test2 on test1.id = test2.id left join db2.test1 on db2.test1.id = db1.test1.id;"
	verifyTestPlanFail(t, sql)
	sql = "select test1.id from test1 left join test2 on test1.id = test2.id left join db2.test1 on db2.test1.id = db1.test1.id;"
	verifyTestPlanFail(t, sql)
	sql = "select db1.test1.id from test1 left join test2 on test1.id = test2.id left join db2.test1 on db2.test1.id = db1.test1.id;"
	verifyTestPlan(t, sql)
}

func TestMakeGroupByPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select id from test1 group by id;"
	verifyTestPlan(t, sql)
	sql = "select id, name from test1 group by id;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 group by id having id > 0;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 group by id having id > 0 order by id limit 2;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 group by id having age > 0;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 group by id having id > 0 order by age;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 group by id having id > 0 order by id limit 2;"
	verifyTestPlan(t, sql)
	sql = "select id, count(name) from test1 group by id;"
	verifyTestPlan(t, sql)
	sql = "select id, count(sum) from test1 group by id;"
	verifyTestPlanFail(t, sql)
	sql = "select id, count(sum) from test1 group by id, name order by id limit 2;"
	verifyTestPlanFail(t, sql)
}

func TestMakeHaveLogicPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select id from test1 having id > 0;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 group by id having id > 0;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 where id > 0 having id < 0;"
	verifyTestPlan(t, sql)
}

func TestMakeLimitLogicPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1 limit 5;"
	verifyTestPlan(t, sql)
	sql = "select * from test1 limit 5, 2;"
	verifyTestPlan(t, sql)
	sql = "select * from test1 limit 5 offset 2;"
	verifyTestPlan(t, sql)
	sql = "select * from test1 limit -1;"
	verifyTestPlanFail(t, sql)
	sql = "select * from test1 limit 2, -1;"
	verifyTestPlanFail(t, sql)
	sql = "select * from test1 limit 5 offset -2;"
	verifyTestPlanFail(t, sql)
	sql = "select * from test1 limit -5 offset 2;"
	verifyTestPlanFail(t, sql)
}

func TestFuncPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1 where max(id) > 0;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 where max(id) > 0;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 having max(id) > 0;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 where max(id) > 0 group by id;"
	verifyTestPlanFail(t, sql)
	sql = "select id from test1 where id > 0 group by id having max(id) > 0;"
	verifyTestPlan(t, sql)
	sql = "select id from test1 where id > 0 group by id having max(max(id)) > 0;"
	verifyTestPlanFail(t, sql)
}
