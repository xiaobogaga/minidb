package lexer

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"simpleDb/log"
	"testing"
	"time"
)

func testOneSql(t *testing.T, sql string) {
	lexer := NewLexer()
	err := lexer.Lex([]byte(sql))
	log.LogDebug("%s\n", lexer)
	assert.Nil(t, err, sql)
}

func testOneSqlShouldErr(t *testing.T, sql string) {
	//
}

type testEntity struct {
	sql    string
	Tokens []Token
}

func TestCreateTable(t *testing.T) {
	sqls := []string{
		"create table  `hello` (id int, name varchar(20), sex bool, location string, c float, f2 float(10, 2));",
		"   create	table 	t_name (	id	int , name	char, age float(10,  	2 ) )",
	}
	for _, sql := range sqls {
		testOneSql(t, sql)
	}
}

func TestCreateDatabase(t *testing.T) {
	sql := " 	create database	`test`		 "
	testOneSql(t, sql)
	sql = "		  create database t_test		;"
	testOneSql(t, sql)
}

func TestDeleteTable(t *testing.T) {
	sql := " 	delete from `t_table` 	;"
	testOneSql(t, sql)
	sql = "delete * from t_table ;"
	testOneSql(t, sql)
	sql = " delete * from t_table where (max(name)==hello and max(age+10) != \"high\") or (cart >=	10 and f < 10.5 and e <= 10 and e > 10 and e < 10) and (hh != true);"
	testOneSql(t, sql)
	sql = " 	delete from table where name == 		hello and age ==		\"wow\" or cart==10;"
	testOneSql(t, sql)
	sql = " 	delete from table where c =='h' and name == 		hello and age ==		\"wow\" or cart==10 and s==10.05;"
	testOneSql(t, sql)
}

func TestDropDatabase(t *testing.T) {
	sql := "	 drop database db 	 ;"
	testOneSql(t, sql)
	sql = "		drop 	database `db`   	;"
	testOneSql(t, sql)
	sql = "	 drop table tb 	 ;"
	testOneSql(t, sql)
	sql = "		drop 	table `db`   	;"
	testOneSql(t, sql)
}

func TestInsert(t *testing.T) {
	sql := " 	insert 	into `tb` values(10, \"hello\", 10.05);"
	testOneSql(t, sql)
	sql = " 	insert 	into `tb` values	( 10, 	 \"hello\", 10.05	 ) 	;"
	testOneSql(t, sql)
	sql = " 	insert 	into tn(name, first) values( 'a', 10+20, \"hello\", 10.05*1);"
	testOneSql(t, sql)
}

func TestSelect(t *testing.T) {
	sql := " 	select * from db; "
	testOneSql(t, sql)
	sql = " 	select name, age + 'c', from `db` where age=10 "
	testOneSql(t, sql)
	sql = "   	select name+ 10, age from db where age 	==10 and c == 'p' or f == 10.09 and name==\"hello\""
	testOneSql(t, sql)
}

func TestUpdate(t *testing.T) {
	sql := " update `tb` set name='c', age='h', sss=\"hello()*\", fff=0.05, i=10 where q=='p' and f==10.05 or name==\"hello ,*()ghllo and i==10;\""
	testOneSql(t, sql)
}

func TestRandomly(t *testing.T) {
	round := 1000
	for i := 0; i < round; i++ {
		sql := generateOneRandomSql()
		testOneSql(t, sql)
	}
}

func generateOneRandomSql() string {
	i := rand.Int63n(time.Now().Unix())
	ret := i % 7
	switch ret {
	case 0:
		return generateCreateSql()
	case 1:
		return generateDeleteSql()
	case 2:
		return generateInsertSql()
	case 3:
		return generateSelectSql()
	case 4:
		return generateUpdateSql()
	case 5:
		return generateRemoveSql()
	case 6:
		return generateDropSql()
	}
	return ""
}

func generateCreateSql() string {
	//Todo
	return ""
}

func generateDeleteSql() string {
	//Todo
	return ""
}

func generateInsertSql() string {
	//Todo
	return ""
}

func generateSelectSql() string {
	//Todo
	return ""
}

func generateUpdateSql() string {
	//Todo
	return ""
}

func generateRemoveSql() string {
	//Todo
	return ""
}

func generateDropSql() string {
	//Todo
	return ""
}
