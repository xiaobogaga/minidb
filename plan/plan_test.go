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
	plan, err := MakeLogicPlan(stm[0].(*parser.SelectStm), "db1")
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
	sql = "select id, name from test1 where id == 1;"
	verifyTestPlan(t, sql)
}
