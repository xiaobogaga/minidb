package parser

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testOneSql(t *testing.T, sql string) {
	lexer := NewLexer()
	_, err := lexer.Lex([]byte(sql))
	assert.Nil(t, err, sql)
}

func testOneSqlButErr(t *testing.T, sql string) {
	lexer := NewLexer()
	_, err := lexer.Lex([]byte(sql))
	assert.NotNil(t, err, sql)
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
	sql = "select count(*) from test;"
	testOneSql(t, sql)
}

func TestUpdate(t *testing.T) {
	sql := " update `tb` set name='c', age='h', sss=\"hello()*\", fff=0.05, i=10 where q=='p' and f==10.05 or name==\"hello ,*()ghllo and i==10;\""
	testOneSql(t, sql)
}

func TestUse(t *testing.T) {
	sql := "use db1;"
	testOneSql(t, sql)
}

func TestShow(t *testing.T) {
	sql := "show databases;"
	testOneSql(t, sql)
	sql = "show tables"
	testOneSql(t, sql)
}

func TestTruncate(t *testing.T) {
	sql := "truncate table t1;"
	testOneSql(t, sql)
}

func TestRename(t *testing.T) {
	sql := "rename table t1 to t2;"
	testOneSql(t, sql)
	sql = "rename database d1 to d2;"
	testOneSql(t, sql)
}

func TestRandomly(t *testing.T) {
	round := 1000
	for i := 0; i < round; i++ {
		sql := generateOneRandomSql()
		testOneSql(t, sql)
	}
}

func TestIdentPattern(t *testing.T) {
	str := "_a1`"
	assert.True(t, identPattern.Match([]byte(str)))
	str = "a11`"
	assert.True(t, identPattern.Match([]byte(str)))
	str = "_a_11`"
	assert.True(t, identPattern.Match([]byte(str)))
	str = "a1111`"
	assert.True(t, identPattern.Match([]byte(str)))
	str = "abas_111`"
	assert.True(t, identPattern.Match([]byte(str)))
	str = "1_asb1`"
	assert.False(t, identPattern.Match([]byte(str)))
}

func TestReadWord(t *testing.T) {
	sql := "a1.a.a"
	testOneSql(t, sql)
	sql = "_a2.a2.a_2"
	testOneSql(t, sql)
	sql = "_a2._a2"
	testOneSql(t, sql)
	sql = "table.database"
	testOneSqlButErr(t, sql)
}

func TestNumericalPattern(t *testing.T) {
	sql := "10.0"
	testOneSql(t, sql)
	sql = "0.0"
	testOneSql(t, sql)
	// sql = ".10"
	// testOneSql(t, sql)
}

func TestLexerIdent(t *testing.T) {
	lexer := NewLexer()
	ts, err := lexer.Lex([]byte("`a1` _ab"))
	assert.Nil(t, err)
	assert.Equal(t, Token{Tp: IDENT, StartPos: 1, EndPos: 3}, ts[0])
	assert.Equal(t, Token{Tp: WORD, StartPos: 5, EndPos: 8}, ts[1])
}

func TestLexerWords(t *testing.T) {
	lexer := NewLexer()
	ts, err := lexer.Lex([]byte("a1.a2.a3 a1.a2 a1 _a3"))
	assert.Nil(t, err)
	assert.Equal(t, Token{Tp: WORD, StartPos: 0, EndPos: 8}, ts[0])
	assert.Equal(t, Token{Tp: WORD, StartPos: 9, EndPos: 14}, ts[1])
	assert.Equal(t, Token{Tp: WORD, StartPos: 15, EndPos: 17}, ts[2])
	assert.Equal(t, Token{Tp: WORD, StartPos: 18, EndPos: 21}, ts[3])
}

func TestLexerValue(t *testing.T) {
	lexer := NewLexer()
	ts, err := lexer.Lex([]byte(" 'hello' \"hi \" true"))
	assert.Nil(t, err)
	assert.Equal(t, Token{Tp: VALUE, StartPos: 1, EndPos: 8}, ts[0])
	assert.Equal(t, Token{Tp: VALUE, StartPos: 9, EndPos: 14}, ts[1])
	assert.Equal(t, Token{Tp: VALUE, StartPos: 15, EndPos: 19}, ts[2])
}

func TestLexerNumerical(t *testing.T) {
	lexer := NewLexer()
	ts, err := lexer.Lex([]byte(" 10 10.0 0.1 10.10"))
	assert.Nil(t, err)
	assert.Equal(t, Token{Tp: VALUE, StartPos: 1, EndPos: 3}, ts[0])
	assert.Equal(t, Token{Tp: VALUE, StartPos: 4, EndPos: 8}, ts[1])
	assert.Equal(t, Token{Tp: VALUE, StartPos: 9, EndPos: 12}, ts[2])
	assert.Equal(t, Token{Tp: VALUE, StartPos: 13, EndPos: 18}, ts[3])
}

func generateOneRandomSql() string {
	i := rand.Int63n(time.Now().Unix())
	ret := i % 8
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
	case 7:

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
