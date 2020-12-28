package plan

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"minidb/parser"
	"minidb/storage"
	"testing"
)

func printTestRecordBatchHeader(record *storage.RecordBatch) {
	buf := bytes.Buffer{}
	for i := 0; i < len(record.Fields); i++ {
		buf.WriteString(record.Fields[i].Name + ",")
	}
	println(buf.String())
}

func printTestRecordBatchRowData(record *storage.RecordBatch, row int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		buf.WriteString(record.Records[i].String(row) + ",")
	}
	println(buf.String())
}

func printTestRecordBatch(record *storage.RecordBatch) {
	// Print header first.
	printTestRecordBatchHeader(record)
	for i := 0; i < record.RowCount(); i++ {
		printTestRecordBatchRowData(record, i)
	}
	println()
}

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
	ret, err := ExecuteShowStm("db1", showStm)
	assert.Nil(t, err)
	printTestRecordBatch(ret)
	showStm.TP = parser.ShowTableTP
	ret, err = ExecuteShowStm("db1", showStm)
	assert.Nil(t, err)
	printTestRecordBatch(ret)
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
	printTestStorage(t)
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
	printTestStorage(t)

	dropTableStm.TableNames = []string{"test2"}
	err = ExecuteDropTableStm(dropTableStm, "db2")
	assert.Nil(t, err)
	printTestStorage(t)
}
