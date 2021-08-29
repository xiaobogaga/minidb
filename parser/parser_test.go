package parser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testSql(t *testing.T, sql string) {
	parse := NewParser()
	stm, err := parse.Parse([]byte(sql))
	assert.Nil(t, err)
	buf := bytes.Buffer{}
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "\t")
	encoder.Encode(stm)
	println(buf.String())
}

func testSqlFail(t *testing.T, sql string) {
	parse := NewParser()
	_, err := parse.Parse([]byte(sql))
	assert.NotNil(t, err)
	fmt.Printf("err: %v\n", err)
}

func TestParser_CreateTable(t *testing.T) {
	sql := "create table t1 (c0 int, c1 int(10), c2 char, c3 char(10), c4 bool, c5 float, c6 float(1), c7 float(2, 1), " +
		"c8 varchar(10), c9 text, c10 mediumtext, c11 blob, c12 mediumblob, c13 datetime);"
	testSql(t, sql)
	sql = "create table t1(c0 int(10), c1 int(10, 2));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 int, c1 int(1), c2(-1));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 char, c1 char(1), c2 char(1, 2));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 float, c1 float(10), c2 float(10, 2));"
	testSql(t, sql)
	sql = "create table t1(c0 char, c1 char(1));"
	testSql(t, sql)
	sql = "create table t1(c0 varchar, c1 varchar(20));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 varchar(1), c1 varchar(20), c2 varchar(10, 1));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 text, c1 text(10));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 mediumtext, c1 mediumtext(10));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 datetime, c1 datetime(10));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 blob, c1 blob(10));"
	testSqlFail(t, sql)
	sql = "create table t1(c0 mediumblob, c1 mediumblob(10));"
	testSqlFail(t, sql)
}

func TestParser_Select(t *testing.T) {
	sql := "select * from test;"
	testSql(t, sql)
	sql = "select c1, c2 from test where i = 10 order by age limit 10;"
	testSql(t, sql)
	sql = "select c1 from where i = 1;"
	testSqlFail(t, sql)
}

func TestParser_Delete(t *testing.T) {
	sql := "delete from test1 where id = 10 and age > 10 limit 2;"
	testSql(t, sql)
	sql = "delete test1 where id = 1;"
	testSqlFail(t, sql)
	sql = "delete from test1 where and age > 10;"
	testSqlFail(t, sql)
}

func TestParser_Update(t *testing.T) {
	sql := "update test1 set id = 10, name='hello' where id = 1;"
	testSql(t, sql)
	sql = "update test1 set where id = 1;"
	testSqlFail(t, sql)
	sql = "update set id = 1;"
	testSqlFail(t, sql)
}

func TestParser_Insert(t *testing.T) {
	sql := "insert into test(c1, c2) values(10, 100);"
	testSql(t, sql)
	sql = "insert into test values(10, 100, 101);"
	testSql(t, sql)
	sql = "insert into test values();"
	testSqlFail(t, sql)
}

func TestParser_CreateDatabase(t *testing.T) {
	sql := "create database db1;"
	testSql(t, sql)
	sql = "create database if not exist db1;"
	testSql(t, sql)
	sql = "create database if not exist db1 character set = utf8;"
	testSql(t, sql)
	sql = "create database;"
	testSqlFail(t, sql)
}

func TestParser_Truncate(t *testing.T) {
	sql := "truncate table tb1;"
	testSql(t, sql)
	sql = "truncate tb1;"
	testSql(t, sql)
	sql = "truncate table tb1, tb2"
	testSqlFail(t, sql)
}

func TestParser_Use(t *testing.T) {
	sql := "use db1;"
	testSql(t, sql)
	sql = "use db"
	testSqlFail(t, sql)
}

func TestParser_Show(t *testing.T) {
	sql := "show databases;"
	testSql(t, sql)
	sql = "show tables;"
	testSql(t, sql)
	sql = "show create table tb1;"
	testSql(t, sql)
}

func TestParser_Rename(t *testing.T) {
	sql := "rename table tb1 to tb2;"
	testSql(t, sql)
	sql = "rename table  tb1 to tb2, tb2 to tb23;"
	testSql(t, sql)
	sql = "rename table tb1 to;"
	testSqlFail(t, sql)
	sql = "rename table tb1 to tb2, tb3;"
	testSqlFail(t, sql)
	sql = "rename table tb1 to tb2;s"
	testSqlFail(t, sql)
}

func TestParser_Drop(t *testing.T) {
	sql := "drop database db1;"
	testSql(t, sql)
	sql = "drop database if exists db1;"
	testSql(t, sql)
	sql = "drop;"
	testSqlFail(t, sql)
	sql = "drop database ;"
	testSqlFail(t, sql)
	sql = "drop database if ;"
	testSqlFail(t, sql)
	sql = "drop database if exists;"
	testSqlFail(t, sql)
	sql = "drop database if exists db1; t"
	testSqlFail(t, sql)
}
