package plan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xiaobogaga/minidb/parser"
	"math"
	"math/rand"
	"testing"
	"time"
)

var randomStrings = []byte("abcdefghijklmnopqrstuvwxyz012345678!@#$%^&*()~{}<>;")

var testDataSize = 4

func generateFloat(random *rand.Rand, max int64) string {
	v := float64(max) * (random.Float64() / random.Float64())
	if v >= float64(max) {
		v = float64(max) - v
	}
	return fmt.Sprintf("%f", v)
}

func generateBool(random *rand.Rand) string {
	i := random.Int()
	if i%2 == 0 {
		return "true"
	}
	return "false"
}

func generateInt(random *rand.Rand) string {
	return fmt.Sprintf("%d", random.Int())
}

var testCharPrefix = "c"

func generateChar(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testCharPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

var testVarcharPrefix = "v"

func generateVarchar(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testVarcharPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

var testTextPrefix = "t"

func generateText(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testTextPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

var testBlobPrefix = "b"

func generateBlob(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testBlobPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

func generateDate(random *rand.Rand) string {
	year := random.Intn(2020)
	month := random.Intn(11) + 1
	day := random.Intn(28) + 1
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func generateTime(random *rand.Rand) string {
	hour := random.Intn(24)
	minute := random.Intn(60)
	second := random.Intn(60)
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}

func generateDateTime(random *rand.Rand) string {
	date := generateDate(random)
	time := generateTime(random)
	return fmt.Sprintf("%s %s", date, time)
}

func generateInsertSql(row int, random *rand.Rand, table string) string {
	colSize := 18
	type colInfo struct {
		value     string
		needQuota bool
	}
	cols := make([]colInfo, colSize)
	cols[0] = colInfo{value: fmt.Sprintf("%d", row), needQuota: false}
	cols[1] = colInfo{value: generateVarchar(random, 20), needQuota: true}
	cols[2] = colInfo{value: generateFloat(random, 100), needQuota: false}
	cols[3] = colInfo{value: fmt.Sprintf("location.%d", row%2), needQuota: true}
	cols[4] = colInfo{value: generateBool(random), needQuota: false}
	cols[5] = colInfo{value: generateInt(random), needQuota: false}
	cols[6] = colInfo{value: generateFloat(random, math.MaxInt64), needQuota: false}
	cols[7] = colInfo{value: generateBool(random), needQuota: false}
	cols[8] = colInfo{value: generateChar(random, 1), needQuota: true}
	cols[9] = colInfo{value: generateChar(random, 20), needQuota: true}
	cols[10] = colInfo{value: generateVarchar(random, 20), needQuota: true}
	cols[11] = colInfo{value: generateDate(random), needQuota: true}
	cols[12] = colInfo{value: generateTime(random), needQuota: true}
	cols[13] = colInfo{value: generateDateTime(random), needQuota: true}
	cols[14] = colInfo{value: generateText(random, 100), needQuota: true}
	cols[15] = colInfo{value: generateText(random, 100), needQuota: true}
	cols[16] = colInfo{value: generateBlob(random, 100), needQuota: true}
	cols[17] = colInfo{value: generateBlob(random, 100), needQuota: true}
	bf := bytes.Buffer{}
	bf.WriteString(fmt.Sprintf("insert into %s values(", table))
	for i := 0; i < colSize; i++ {
		if i != colSize-1 {
			if cols[i].needQuota {
				bf.WriteString(fmt.Sprintf("'%s', ", cols[i].value))
			} else {
				bf.WriteString(fmt.Sprintf("%s, ", cols[i].value))
			}
		} else {
			if cols[i].needQuota {
				bf.WriteString(fmt.Sprintf("'%s'", cols[i].value))
			} else {
				bf.WriteString(cols[i].value)
			}
		}
	}
	bf.WriteString(");")
	return bf.String()
}

func generateInsert(t *testing.T, row int, random *rand.Rand, currentDB *string, table string) {
	parser := parser.NewParser()
	sql := generateInsertSql(row, random, table)
	stm, err := parser.Parse([]byte(sql))
	assert.Nil(t, err, sql)
	exec, err := MakeExecutor(stm, currentDB)
	assert.Nil(t, err, sql)
	_, err = exec.Exec()
	assert.Nil(t, err, sql)
}

func initTestStorage(t *testing.T) {
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
		"create table test2(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
		"create database db2;",
		"use db2;",
		"create table test1(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
		"create table test2(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
	}
	parser := parser.NewParser()
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
		generateInsert(t, i, random, &currentDB, "test1")
		generateInsert(t, i, random, &currentDB, "test2")
	}
	currentDB = "db2"
	// insert some data to db2 tables.
	for i := 0; i < testDataSize; i++ {
		generateInsert(t, i, random, &currentDB, "test1")
		generateInsert(t, i, random, &currentDB, "test2")
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
