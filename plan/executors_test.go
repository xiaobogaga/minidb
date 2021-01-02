package plan

import (
	"fmt"
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

func assertRecordBatch(t *testing.T, rowSize, columnSize int, ret *storage.RecordBatch) {
	assert.Equal(t, rowSize, ret.RowCount())
	assert.Equal(t, columnSize, ret.ColumnCount())
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
	assertRecordBatch(t, 2, 2, ret)
	storage.PrintRecordBatch(ret, true)
	showStm.TP = parser.ShowTableTP
	showPlan = &Show{}
	ret, err = showPlan.Execute("db1", showStm)
	assert.Nil(t, err)
	storage.PrintRecordBatch(ret, true)
	assertRecordBatch(t, 2, 2, ret)
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
	showStm := &parser.ShowStm{TP: parser.ShowDatabaseTP}
	showPlan := &Show{}
	ret, err := showPlan.Execute("db1", showStm)
	assert.Nil(t, err)
	assertRecordBatch(t, 0, 2, ret)
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

func testSelect(t *testing.T, sql string, expectRowSize int, expectErr bool) {
	stm := toTestStm(t, sql)
	db := "db1"
	exec, err := MakeExecutor(stm.(*parser.SelectStm), &db)
	if expectErr && err != nil {
		fmt.Printf("err: %s\n", err)
		return
	}
	assert.Nil(t, err)
	println("sql: ", sql)
	i := 0
	count := 0
	for {
		var ret *storage.RecordBatch
		ret, err = exec.Exec()
		if err != nil {
			break
		}
		if ret == nil {
			break
		}
		count += ret.RowCount()
		storage.PrintRecordBatch(ret, i == 0)
		i++
	}
	assert.Equal(t, expectRowSize, count)
	if expectErr {
		assert.True(t, err != nil)
		fmt.Printf("err: %s\n", err)
	} else {
		assert.Nil(t, err)
	}
}

func testSelect2(t *testing.T, sql string) {
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
	testSelect(t, sql, testDataSize, false)

	sql = "select * from test1 where id = 0;"
	testSelect(t, sql, 1, false)

	sql = "select * from test1 where id = 1;"
	testSelect(t, sql, 1, false)

	sql = "select * from test1 where (id = 1 + 1 or id = 1);"
	testSelect(t, sql, 2, false)

	sql = "select * from test1 where (id = 2 or id = 1) and name='hello';"
	testSelect(t, sql, 0, false)

	sql = "select id from test1 where (id = 2 or id = 1) and name='hello';"
	testSelect(t, sql, 0, false)

	sql = "select * from test1 where id % 3 = 0 order by id desc;"
	testSelect(t, sql, testDataSize/3+1, false)
}

func TestExecuteSelectStmWithJoin(t *testing.T) {
	initTestStorage(t)
	sql := "select * from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select * from test2;"
	testSelect(t, sql, testDataSize, false)
	sql = "select * from test1, test2;"
	testSelect(t, sql, testDataSize*testDataSize, false)
	sql = "select * from test1 left join test2 on test1.age > test2.age order by test2.age;"
	testSelect2(t, sql)
	sql = "select test1.id from test1 left join test2 on test1.age > test2.age where test1.id = 1 or test1.id = 2 limit 1;"
	testSelect2(t, sql)
	sql = "select * from test1, test2, db2.test1;"
	testSelect2(t, sql)
}

func TestExecuteSelectWithOrderBy(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select id, age, location from test1 order by location desc, id;"
	testSelect(t, sql, testDataSize, false)
}

func TestExecuteSelectWithLimit(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select * from test1 limit 2 offset 1;"
	testSelect(t, sql, 2, false)
	sql = "select * from test1 limit 1 offset 2;"
	testSelect(t, sql, 1, false)
	sql = "select * from test1 limit 2, 1;"
	testSelect(t, sql, 1, false)
	sql = "select id, name, age, location from test1 where id = 1 or id = 2 limit 1;"
	testSelect(t, sql, 1, false)
	sql = "select * from test1 limit 1, 1000;"
	testSelect(t, sql, 3, false)
}

func TestExecuteSelectWithGroupBy(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select location from test1 group by location;"
	testSelect(t, sql, testDataSize/2, false)
	sql = "select location from test1 group by location order by location desc limit 1;"
	testSelect(t, sql, 1, false)
	sql = "select location from test1 group by location order by location desc;"
	testSelect(t, sql, testDataSize/2, false)
	sql = "select sum(id), location from test1 group by location;"
	testSelect(t, sql, testDataSize/2, false)
	sql = "select max(id), location from test1 group by location;"
	testSelect(t, sql, testDataSize/2, false)
	sql = "select min(id), location from test1 group by location;"
	testSelect(t, sql, testDataSize/2, false)
	sql = "select count(id), location from test1 group by location;"
	testSelect(t, sql, testDataSize/2, false)
	sql = "select id from test1 group by id order by id desc limit 2;"
	testSelect(t, sql, 2, false)
	sql = "select id, age from test1 group by id, age order by id, age limit 1;"
	testSelect(t, sql, 1, false)
	// Now we test group by and have.
	sql = "select id from test1 having id > 0;"
	testSelect(t, sql, 3, false)
	sql = "select id, count(name) from test1 group by id having id > 0;"
	testSelect(t, sql, 3, false)

	// Now we do some fail check.
	sql = "select * from test1 group by id;"
	testSelect(t, sql, 0, true)
	sql = "select id, name from test1 group by id;"
	testSelect(t, sql, 0, true)
	sql = "select id, name, age from test1 group by id, age;"
	testSelect(t, sql, 0, true)
	sql = "select id, name, age from test1 group by id, age order by id desc limit 1;"
	testSelect(t, sql, 0, true)

}

func TestExecuteHavingPlan(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1 having id > 0;"
	testSelect(t, sql, testDataSize-1, false)
	sql = "select * from test1 where id > 0 having id > 0;"
	testSelect(t, sql, testDataSize-1, false)
	sql = "select * from test1 where id < 0 having id > 0;"
	testSelect(t, sql, 0, false)
	sql = "select id from test1 having id > 0 order by id limit 2;"
	testSelect(t, sql, 2, false)
}

func TestExecuteFuncCall(t *testing.T) {
	initTestStorage(t)
	var sql string
	sql = "select * from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select id, name, charlength(name) from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select location, count(name) from test1 group by location;"
	testSelect(t, sql, 2, false)
	sql = "select location, max(name) from test1 group by location;"
	testSelect(t, sql, 2, false)
	sql = "select location, min(name) from test1 group by location;"
	testSelect(t, sql, 2, false)
	// Test several fails.
	sql = "select sum(name) from test1 group by location;"
	testSelect(t, sql, 0, true)
	sql = "select count(id, age) from test1 group by id;"
	testSelect(t, sql, 0, true)
	sql = "select max(id, age) from test1 group by id;"
	testSelect(t, sql, 0, true)
	sql = "select sum(age) from test1;"
	testSelect(t, sql, 1, false)
	sql = "select max(id) from test1;"
	testSelect(t, sql, 1, false)
	sql = "select max(id), min(id), count(name) from test1;"
	testSelect(t, sql, 1, false)
}

func TestExecuteSelectWithLargeData(t *testing.T) {
	batchSize = 4
	testDataSize = batchSize * 3
	initTestStorage(t)
	sql := "select * from test1;"
	testSelect(t, sql, testDataSize, false)
	sql = "select * from test1 where id % 3 = 0 order by id desc;"
	testSelect(t, sql, testDataSize/3, false)
	sql = "select id from test1 where id % 3 = 0 order by id;"
	testSelect(t, sql, testDataSize/3, false)
	sql = "select id, age, location from test1 order by location desc, id;"
	testSelect(t, sql, testDataSize, false)
	sql = "select id, age from test1 order by age limit 2, 8;"
	testSelect(t, sql, 8, false)
}
