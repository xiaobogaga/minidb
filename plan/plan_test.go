package plan

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"minidb/parser"
	"testing"
)

func TestLogicPlan(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1;"
	p := parser.NewParser()
	stm, err := p.Parse([]byte(sql))
	assert.Nil(t, err)
	plan, err := MakeLogicPlan(stm[0].(*parser.SelectStm), "db1")
	assert.Nil(t, err)
	data, err := json.MarshalIndent(plan, "", "\t")
	assert.Nil(t, err)
	println(string(data))
}
