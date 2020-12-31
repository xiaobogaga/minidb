package plan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"minidb/parser"
	"minidb/storage"
	"testing"
)

func printTestColumn(t *testing.T, col storage.Field, padding string) {
	fmt.Printf("%sColumn: %s.%s.%s, TP: %s, primaryKey: %v, allowNull: %v, autoInc: %v.\n", padding, col.SchemaName,
		col.TableName, col.Name, col.TP, col.PrimaryKey, col.AllowNull, col.AutoIncrement)
}

func printTestTableRowData(t *testing.T, tableInfo *storage.TableInfo, row int, padding string) {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("%s", padding))
	for i, col := range tableInfo.Datas {
		if i == 0 {
			continue
		}
		buf.WriteString(col.String(row) + ", ")
	}
	println(buf.String())
}

func printTestTableHeader(t *testing.T, tableInfo *storage.TableInfo, padding string) {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("%s", padding))
	for i, col := range tableInfo.Datas {
		if i == 0 {
			continue
		}
		// Wont print row index column.
		buf.WriteString(col.Field.Name + " ,")
	}
	println(buf.String())
}

func printTestTable(t *testing.T, tableInfo *storage.TableInfo, padding string) {
	fmt.Printf("%sTable: %s.%s, collate: %s, charset: %s.\n", padding, tableInfo.TableSchema.SchemaName(),
		tableInfo.TableSchema.TableName(), tableInfo.Collate, tableInfo.Charset)
	// Now we can print table column definitions.
	for _, col := range tableInfo.TableSchema.Columns {
		printTestColumn(t, col, padding+padding)
	}
	fmt.Printf("%sTable data:\n", padding)
	printTestTableHeader(t, tableInfo, padding+padding)
	// Now print Test data
	for i := 0; i < tableInfo.Datas[1].Size(); i++ {
		printTestTableRowData(t, tableInfo, i, padding+padding)
	}
	println("")
}

func printTestStorage(t *testing.T) {
	storage := storage.GetStorage()
	for _, dbSchema := range storage.Dbs {
		fmt.Printf("dbInfo: %s, collate: %s, charset: %s.\n", dbSchema.Name, dbSchema.Collate, dbSchema.Charset)
		for _, table := range dbSchema.Tables {
			printTestTable(t, table, "\t")
		}
	}
}

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
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
	currentDB = "db1"
	// insert some data to db1 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1);", i, i, testDataSize-(i*int(rand.Int31n(10))))
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1);", i, i, testDataSize-(i*int(rand.Int31n(10))))
		stm, err = parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err = MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
	currentDB = "db2"
	// insert some data to db2 tables.
	for i := 0; i < testDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1);", i, i, testDataSize-(i*int(rand.Int31n(10))))
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err := MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1);", i, i, testDataSize-(i*int(rand.Int31n(10))))
		stm, err = parser.Parse([]byte(sql))
		assert.Nil(t, err)
		exec, err = MakeExecutor(stm, &currentDB)
		assert.Nil(t, err)
		_, err = exec.Exec()
		assert.Nil(t, err)
	}
}

func verifyTestExpr(t *testing.T, sql string) {
	parser := parser.NewParser()
	parser.Set([]byte(sql))
	stm, err := parser.ResolveWhereStm()
	assert.Nil(t, err)
	expr := ExprStmToLogicExpr(stm, nil)
	data, err := json.MarshalIndent(expr, "", "\t")
	assert.Nil(t, err)
	println(string(data))
}

func TestMakeLogicExpr(t *testing.T) {
	sql := "where id=1 * name + c % 1"
	verifyTestExpr(t, sql)
	sql = "where max(id, age, sum(name)) + max(id) + id * 5"
	verifyTestExpr(t, sql)
}

func TestLogicExpr_TypeCheck(t *testing.T) {
	initTestStorage(t)
	sql := "select * from db1.test1 where id = 1 + 0;"
	testSelect(t, sql)
	sql = "select * from db1.test1 where id = 1 + 0 or id = 2;"
	testSelect(t, sql)
}
