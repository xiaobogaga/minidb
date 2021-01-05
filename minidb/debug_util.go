package main

import (
	"bytes"
	"fmt"
	"github.com/xiaobogaga/minidb/parser"
	"github.com/xiaobogaga/minidb/plan"
	"math"
	"math/rand"
	"time"
)

var randomStrings = []byte("abcdefghijklmnopqrstuvwxyz012345678!@#$%^&*()~{}<>;")

var testDataSize = 4

func generateFloat(random *rand.Rand, max int64) string {
	v := float64(max) * (random.Float64() / random.Float64())
	if v >= float64(max) {
		v = float64(max) - v
	}
	return fmt.Sprintf("%f", v)
}

func generateBool(random *rand.Rand) string {
	i := random.Int()
	if i%2 == 0 {
		return "true"
	}
	return "false"
}

func generateInt(random *rand.Rand) string {
	return fmt.Sprintf("%d", random.Int())
}

var testCharPrefix = "c"

func generateChar(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testCharPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

var testVarcharPrefix = "v"

func generateVarchar(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testVarcharPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

var testTextPrefix = "t"

func generateText(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testTextPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

var testBlobPrefix = "b"

func generateBlob(random *rand.Rand, size int) string {
	bf := bytes.Buffer{}
	bf.WriteString(testBlobPrefix)
	size = random.Intn(size)
	for i := 1; i < size; i++ {
		bf.WriteString(fmt.Sprintf("%c", randomStrings[random.Intn(51)]))
	}
	return bf.String()
}

func generateDate(random *rand.Rand) string {
	year := random.Intn(2020)
	month := random.Intn(11) + 1
	day := random.Intn(28) + 1
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func generateTime(random *rand.Rand) string {
	hour := random.Intn(24)
	minute := random.Intn(60)
	second := random.Intn(60)
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}

func generateDateTime(random *rand.Rand) string {
	date := generateDate(random)
	time := generateTime(random)
	return fmt.Sprintf("%s %s", date, time)
}

func generateInsertSql(row int, random *rand.Rand, table string) string {
	colSize := 18
	type colInfo struct {
		value     string
		needQuota bool
	}
	cols := make([]colInfo, colSize)
	cols[0] = colInfo{value: fmt.Sprintf("%d", row), needQuota: false}
	cols[1] = colInfo{value: generateVarchar(random, 20), needQuota: true}
	cols[2] = colInfo{value: generateFloat(random, 100), needQuota: false}
	cols[3] = colInfo{value: fmt.Sprintf("location.%d", row%2), needQuota: true}
	cols[4] = colInfo{value: generateBool(random), needQuota: false}
	cols[5] = colInfo{value: generateInt(random), needQuota: false}
	cols[6] = colInfo{value: generateFloat(random, math.MaxInt64), needQuota: false}
	cols[7] = colInfo{value: generateBool(random), needQuota: false}
	cols[8] = colInfo{value: generateChar(random, 1), needQuota: true}
	cols[9] = colInfo{value: generateChar(random, 20), needQuota: true}
	cols[10] = colInfo{value: generateVarchar(random, 20), needQuota: true}
	cols[11] = colInfo{value: generateDate(random), needQuota: true}
	cols[12] = colInfo{value: generateTime(random), needQuota: true}
	cols[13] = colInfo{value: generateDateTime(random), needQuota: true}
	cols[14] = colInfo{value: generateText(random, 100), needQuota: true}
	cols[15] = colInfo{value: generateText(random, 100), needQuota: true}
	cols[16] = colInfo{value: generateBlob(random, 100), needQuota: true}
	cols[17] = colInfo{value: generateBlob(random, 100), needQuota: true}
	bf := bytes.Buffer{}
	bf.WriteString(fmt.Sprintf("insert into %s values(", table))
	for i := 0; i < colSize; i++ {
		if i != colSize-1 {
			if cols[i].needQuota {
				bf.WriteString(fmt.Sprintf("'%s', ", cols[i].value))
			} else {
				bf.WriteString(fmt.Sprintf("%s, ", cols[i].value))
			}
		} else {
			if cols[i].needQuota {
				bf.WriteString(fmt.Sprintf("'%s'", cols[i].value))
			} else {
				bf.WriteString(cols[i].value)
			}
		}
	}
	bf.WriteString(");")
	return bf.String()
}

func generateInsert(row int, random *rand.Rand, currentDB *string, table string) {
	parser := parser.NewParser()
	sql := generateInsertSql(row, random, table)
	stm, err := parser.Parse([]byte(sql))
	panicErr(err)
	exec, err := plan.MakeExecutor(stm, currentDB)
	panicErr(err)
	_, err = exec.Exec()
	panicErr(err)
}

func initDataForDebug() {
	batch := 4
	plan.SetBatchSize(batch)
	testDataSize = batch * batch
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
		"create table test2(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
		"create database db2;",
		"use db2;",
		"create table test1(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
		"create table test2(" +
			"id int primary key, " +
			"name varchar(20), " +
			"age float, " +
			"location varchar(20), " +
			"sex bool, " +
			"c1 int(10), " +
			"c2 float(10, 2), " +
			"c3 bool, " +
			"c4 char, " +
			"c5 char(20), " +
			"c6 varchar(20), " +
			"c7 date, " +
			"c8 time, " +
			"c9 datetime, " +
			"c10 text, " +
			"c11 mediumtext, " +
			"c12 blob, " +
			"c13 mediumblob);",
	}
	parser := parser.NewParser()
	currentDB := ""
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql))
		panicErr(err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		panicErr(err)
		_, err = exec.Exec()
		panicErr(err)
	}
	random := rand.New(rand.NewSource(time.Now().Unix()))
	currentDB = "db1"
	// insert some data to db1 tables.
	for i := 0; i < testDataSize; i++ {
		generateInsert(i, random, &currentDB, "test1")
		generateInsert(i, random, &currentDB, "test2")
	}
	currentDB = "db2"
	// insert some data to db2 tables.
	for i := 0; i < testDataSize; i++ {
		generateInsert(i, random, &currentDB, "test1")
		generateInsert(i, random, &currentDB, "test2")
	}
}
