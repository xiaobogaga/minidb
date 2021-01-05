package protocol

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xiaobogaga/parser"
	"github.com/xiaobogaga/plan"
	"github.com/xiaobogaga/storage"
	"github.com/xiaobogaga/util"
	"testing"
	"time"
)

const testDataSize = 4

func initTestStorage(t *testing.T) {
	parser := parser.NewParser()
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(id int primary key, name varchar(20), age float);",
		"create table test2(id int primary key, name varchar(20), age float);",
		"create database db2;",
		"use db2;",
		"create table test1(id int primary key, name varchar(20), age float);",
		"create table test2(id int primary key, name varchar(20), age float);",
	}
	currentDB := ""
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
	currentDB = "db1"
	// insert some data to db1 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1);", i, i, i)
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1);", i, i, i)
		stm, err = parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err = plan.MakeExecutor(stm, &currentDB)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
	currentDB = "db2"
	// insert some data to db2 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1);", i, i, i)
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1);", i, i, i)
		stm, err = parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err = plan.MakeExecutor(stm, &currentDB)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
}

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
	if record == nil {
		return
	}
	// Print header first.
	printTestRecordBatchHeader(record)
	for i := 0; i < record.RowCount(); i++ {
		printTestRecordBatchRowData(record, i)
	}
	println()
}

type connectionWrapperForTest struct {
	db string
}

func (con *connectionWrapperForTest) CurrentDB() *string {
	return &con.db
}

func (con *connectionWrapperForTest) SendErrMsg(msg ErrMsg) {
	println("send err msg")
}

func (con *connectionWrapperForTest) SendQueryResult(ret *storage.RecordBatch) ErrMsg {
	printTestRecordBatch(ret)
	return OkMsg
}

func TestComQuery_Do(t *testing.T) {
	util.InitLogger("", 1024, time.Second, true)
	con := &connectionWrapperForTest{db: "db1"}
	commandQuery := ComQuery("test")
	sql := "select * from test1;"
	commandQuery.Do(con, []byte(sql))
}
