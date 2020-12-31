package plan

import (
	"github.com/stretchr/testify/assert"
	"minidb/parser"
	"minidb/storage"
	"testing"
)

func TestExecuteUseStm(t *testing.T) {
	initTestStorage(t)
	useStm := &parser.UseDatabaseStm{DatabaseName: "db1"}
	err := ExecuteUseStm(useStm)
	assert.Nil(t, err)
	useStm.DatabaseName = "db3"
	err = ExecuteUseStm(useStm)
	assert.NotNil(t, err)
}

func TestExecuteShowStm(t *testing.T) {
	initTestStorage(t)
	useStm := &parser.UseDatabaseStm{DatabaseName: "db1"}
	err := ExecuteUseStm(useStm)
	assert.Nil(t, err)
	showStm := &parser.ShowStm{TP: parser.ShowDatabaseTP}
	showPlan := &Show{}
	ret, err := showPlan.Execute("db1", showStm)
	assert.Nil(t, err)
	storage.PrintRecordBatch(ret, true)
	showStm.TP = parser.ShowTableTP
	showPlan = &Show{}
	ret, err = showPlan.Execute("db1", showStm)
	assert.Nil(t, err)
	storage.PrintRecordBatch(ret, true)
}

func TestExecuteDropDatabaseStm(t *testing.T) {
	initTestStorage(t)
	dropStm := &parser.DropDatabaseStm{
		DatabaseName: "db",
	}
	err := ExecuteDropDatabaseStm(dropStm)
	assert.NotNil(t, err)
	dropStm.DatabaseName = "db1"
	err = ExecuteDropDatabaseStm(dropStm)
	assert.Nil(t, err)
	dropStm.DatabaseName = "db2"
	err = ExecuteDropDatabaseStm(dropStm)
	assert.Nil(t, err)
	storage.PrintStorage(t)
}

func TestExecuteDropTableStm(t *testing.T) {
	initTestStorage(t)
	dropTableStm := &parser.DropTableStm{
		IfExists:   false,
		TableNames: []string{"testtttt"},
	}
	err := ExecuteDropTableStm(dropTableStm, "")
	assert.NotNil(t, err)
	dropTableStm.TableNames = []string{"test1"}
	err = ExecuteDropTableStm(dropTableStm, "db1")
	assert.Nil(t, err)
	storage.PrintStorage(t)

	dropTableStm.TableNames = []string{"test2"}
	err = ExecuteDropTableStm(dropTableStm, "db2")
	assert.Nil(t, err)
	storage.PrintStorage(t)
}

func toTestStm(t *testing.T, sql string) parser.Stm {
	parser := parser.NewParser()
	stm, err := parser.Parse([]byte(sql))
	assert.Nil(t, err)
	return stm
}

func testSelect(t *testing.T, sql string) {
	stm := toTestStm(t, sql)
	db := "db1"
	exec, err := MakeExecutor(stm.(*parser.SelectStm), &db)
	assert.Nil(t, err)
	println("sql: ", sql)
	i := 0
	for {
		ret, err := exec.Exec()
		assert.Nil(t, err)
		if ret == nil {
			break
		}
		storage.PrintRecordBatch(ret, i == 0)
		i++
	}
}

func TestExecuteSelectStm(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1;"
	testSelect(t, sql)

	sql = "select * from test1 where id = 0;"
	testSelect(t, sql)

	sql = "select * from test1 where id = 1;"
	testSelect(t, sql)

	sql = "select * from test1 where (id = 1 + 1 or id = 1);"
	testSelect(t, sql)

	sql = "select * from test1 where (id = 2 or id = 1) and name='hello';"
	testSelect(t, sql)

	sql = "select id from test1 where (id = 2 or id = 1) and name='hello';"
	testSelect(t, sql)

	sql = "select * from test1 where id % 3 = 0 order by id desc;"
	testSelect(t, sql)
}

func TestExecuteSelectStmWithJoin(t *testing.T) {

}

func TestExecuteSelectWithOrderBy(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1;"
	testSelect(t, sql)
	sql = "select id, age, location from test1 order by location desc, id;"
	testSelect(t, sql)
}

func TestExecuteSelectWithLargeData(t *testing.T) {
	batchSize = 4
	testDataSize = batchSize * 3
	initTestStorage(t)
	sql := "select * from test1;"
	testSelect(t, sql)
	sql = "select * from test1 where id % 3 = 0 order by id desc;"
	testSelect(t, sql)
	sql = "select id from test1 where id % 3 = 0 order by id;"
	testSelect(t, sql)
	sql = "select id, age, location from test1 order by location desc, id;"
	testSelect(t, sql)
	sql = "select id, age from test1 order by age limit 8, 2;"
	testSelect(t, sql)
}

func TestExecuteSelectWithLimit(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1;"
	testSelect(t, sql)
	sql = "select * from test1 limit 2 offset 1;"
	testSelect(t, sql)
	sql = "select * from test1 limit 1 offset 2;"
	testSelect(t, sql)
	sql = "select * from test1 limit 2, 1;"
	testSelect(t, sql)
	sql = "select id, name, age, location from test1 where id = 1 or id = 2 limit 1;"
	testSelect(t, sql)
}
