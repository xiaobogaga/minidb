package plan

import (
	"github.com/stretchr/testify/assert"
	"minidb/parser"
	"testing"
)

func TestLogicPlan(t *testing.T) {
	sql := "select * from test;"
	parser := parser.NewParser()
	stm, err := parser.Parse([]byte(sql))
	assert.Nil(t, err)
	MakeLogicPlan(stm[0].(*parser.SelectStm), "db1")
}
