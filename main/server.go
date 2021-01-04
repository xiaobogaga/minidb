package main

import (
	"fmt"
	"math/rand"
	"minidb/parser"
	"minidb/plan"
	"minidb/protocol"
	"minidb/util"
	"time"
)

func panicErr(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func initDataForDebug() {
	batch := 4
	plan.SetBatchSize(4)
	debugDataSize := batch * batch
	parser := parser.NewParser()
	sqls := []string{
		"create database db1;",
		"use db1;",
		"create table test1(id int primary key, name varchar(20), age float, location varchar(20));",
		"create table test2(id int primary key, name varchar(20), age float, location varchar(20));",
		"create database db2;",
		"use db2;",
		"create table test1(id int primary key, name varchar(20), age float, location varchar(20));",
		"create table test2(id int primary key, name varchar(20), age float, location varchar(20));",
	}
	currentDB := ""
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql))
		panicErr(err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		panicErr(err)
		_, err = exec.Exec()
		panicErr(err)
	}
	currentDB = "db1"
	// insert some data to db1 tables.
	for i := 0; i < debugDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1, '%d');", i, i, debugDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err := parser.Parse([]byte(sql))
		panicErr(err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		panicErr(err)
		_, err = exec.Exec()
		panicErr(err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1, '%d');", i, i, debugDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err = parser.Parse([]byte(sql))
		panicErr(err)
		exec, err = plan.MakeExecutor(stm, &currentDB)
		panicErr(err)
		_, err = exec.Exec()
		panicErr(err)
	}
	currentDB = "db2"
	// insert some data to db2 tables.
	for i := 0; i < debugDataSize; i++ {
		sql := fmt.Sprintf("insert into test1 values(%d, '%d', %d.1, '%d');", i, i, debugDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err := parser.Parse([]byte(sql))
		panicErr(err)
		exec, err := plan.MakeExecutor(stm, &currentDB)
		panicErr(err)
		_, err = exec.Exec()
		panicErr(err)
		sql = fmt.Sprintf("insert into test2 values(%d, '%d', %d.1, '%d');", i, i, debugDataSize-(i*int(rand.Int31n(10))), i%2)
		stm, err = parser.Parse([]byte(sql))
		panicErr(err)
		exec, err = plan.MakeExecutor(stm, &currentDB)
		panicErr(err)
		_, err = exec.Exec()
		panicErr(err)
	}
}

func initServer() {
	log := util.GetLog("server")
	if *debug {
		log.InfoF("init debug data")
		initDataForDebug()
	}
	server := protocol.NewServerWithTimeout(*port, time.Millisecond*(time.Duration(*readTimeout)),
		time.Millisecond*(time.Duration(*writeTimeout)), *unixSocket)
	server.Start()
}
