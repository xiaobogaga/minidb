package plan

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"minidb/parser"
	"testing"
)

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
