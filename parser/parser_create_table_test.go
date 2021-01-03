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
