package plan

import (
	"github.com/stretchr/testify/assert"
	"minidb/parser"
	"minidb/storage"
	"testing"
)

func testInsert(t *testing.T, sql string) {
	stm := toTestStm(t, sql)
	err := ExecuteInsertStm(stm.(*parser.InsertIntoStm), "db1")
	assert.Nil(t, err)
	storage.PrintStorage(t)
}

func testInsertFail(t *testing.T, sql string) {
	stm := toTestStm(t, sql)
	err := ExecuteInsertStm(stm.(*parser.InsertIntoStm), "db1")
	assert.NotNil(t, err)
	storage.PrintStorage(t)
}

func TestInsert_Execute(t *testing.T) {
	initTestStorage(t)
	// testInsert(t, generateInsertSql(1))
	//sql = "insert into db1.test1 values(10, 'xiaoboxxxxxxxxxxxxxxxxxxxxxxxxx', 10.0, 'a', 0);"
	//testInsertFail(t, sql)
}

func testUpdate(t *testing.T, sql string) {
	stm := toTestStm(t, sql)
	err := ExecuteUpdateStm(stm.(*parser.UpdateStm), "db1")
	assert.Nil(t, err)
	storage.PrintStorage(t)
}

func TestUpdate_Execute(t *testing.T) {
	initTestStorage(t)
	sql := "update db1.test1 set name='xxxxx' where id = 1 or id = 2;"
	testUpdate(t, sql)
}

func TestMultiUpdate_Execute(t *testing.T) {

}

func testDelete(t *testing.T, sql string) {
	stm := toTestStm(t, sql)
	err := ExecuteDeleteStm(stm.(*parser.SingleDeleteStm), "db1")
	assert.Nil(t, err)
	storage.PrintStorage(t)
}

func TestDelete_Execute(t *testing.T) {
	initTestStorage(t)
	sql := "delete from db1.test1 where id = 1 or id = 2;"
	testDelete(t, sql)
}

func TestMultiDelete_Execute(t *testing.T) {

}
