package plan

import (
	"github.com/stretchr/testify/assert"
	"simpleDb/parser"
	"testing"
)

func initTestStorage(t *testing.T) {
	parser := parser.NewParser()
	sqls := []string{
		"create database test;",
		"use test;",
		"create table test1(id int primary key, name varchar(20));",
		"create table test2(id int primary key, name varchar(20));",
		"insert into test1 values(1, 'hello');",
		"insert into test2 values(2, 'hi');",
	}
	currentDB := ""
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql))
		assert.Nil(t, err)
		_, newCurrentDB, err := Exec(stm[0], currentDB)
		if newCurrentDB != "" {
			currentDB = newCurrentDB
		}
		assert.Nil(t, err)
	}
}

func TestLogicExpr(t *testing.T) {
	initTestStorage(t)
}
