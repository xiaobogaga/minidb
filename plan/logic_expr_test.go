package plan

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"simpleDb/parser"
	"simpleDb/storage"
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

func initTestStorage(t *testing.T) {
	parser := parser.NewParser()
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(id int primary key, name varchar(20));",
		"create table test2(id int primary key, name varchar(20));",
		"insert into test1 values(1, 'hello');",
		"insert into test2 values(2, 'hi');",
		"insert into test1(id, name) values(2, 'hi');",
		"insert into test1 values(1 * 2 + (2 * 3 + 3), 'hi');",
		"create database db2;",
		"use db2;",
		"create table test1(id int primary key, name varchar(20));",
		"create table test2(id int primary key, name varchar(20));",
		"insert into test1 values(1, 'hello');",
		"insert into test2 values(2, 'hi');",
		"insert into test1(id, name) values(2, 'hi');",
		"insert into test1 values(1 * 2 + (2 * 3 + 3), 'hi');",
	}
	currentDB := ""
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		_, finish, err := Exec(stm[0], &currentDB)
		assert.True(t, finish)
		assert.Nil(t, err)
	}
	printTestStorage(t)
}

func deleteTestStore(t *testing.T) {

}

func updateTestStore(t *testing.T) {

}

func TestLogicExpr(t *testing.T) {
	initTestStorage(t)
}
