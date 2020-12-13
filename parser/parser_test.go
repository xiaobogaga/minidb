package parser

//
//import (
//	"github.com/stretchr/testify/assert"
//	"simpleDb/ast"
//	"simpleDb/lexer"
//	"testing"
//)
//
//type testEntity struct {
//	sql string
//	stm ast.Stm
//}
//
//func testSqls(t *testing.T, sqls []testEntity) {
//	parser := NewParser()
//	for _, sql := range sqls {
//		stm, err := parser.Parse([]byte(sql.sql))
//		//log.LogDebug("%+v\n", stm)
//		assert.Nil(t, err, sql)
//		assert.Equal(t, sql.stm, stm.Stms[0], sql)
//	}
//}
//
//func TestCreate(t *testing.T) {
//	sqls := []testEntity{
//		{
//			"create table IF NOT EXIST tb_1" +
//				"(" +
//				"id int default 0 primary key, " +
//				"name varchar( 20) default \"hello\", " +
//				"age float(10, 2) default 10, " +
//				"location string default \"haha\", " +
//				"age char default 'z'" +
//				");",
//			&ast.CreateTableStm{
//				TableName:  "tb_1",
//				IfNotExist: true,
//				Cols: []*ast.ColumnDefStm{
//					{ColName: "id", ColumnType: ast.ColumnType{Tp: lexer.INT}, PrimaryKey: true, ColDefaultValue: ast.ColumnValue{Value: 0, ValueType: lexer.INTVALUE}},
//					{ColName: "name", ColumnType: ast.ColumnType{Tp: lexer.VARCHAR, Min: 20}, ColDefaultValue: ast.ColumnValue{Value: "hello", ValueType: lexer.STRINGVALUE}},
//					{ColName: "age", ColumnType: ast.ColumnType{Tp: lexer.FLOAT, Min: 10, Max: 2}, ColDefaultValue: ast.ColumnValue{Value: 10, ValueType: lexer.INTVALUE}},
//					{ColName: "location", ColumnType: ast.ColumnType{Tp: lexer.STRING}, ColDefaultValue: ast.ColumnValue{Value: "haha", ValueType: lexer.STRINGVALUE}},
//					{ColName: "age", ColumnType: ast.ColumnType{Tp: lexer.CHAR}, ColDefaultValue: ast.ColumnValue{Value: byte('z'), ValueType: lexer.CHARVALUE}},
//				},
//			},
//		},
//		{
//			"create table tb_1" +
//				"(" +
//				"id int default 0 primary key, " +
//				"name varchar( 20) default \"hello\", " +
//				"age float(10, 2) default 10, " +
//				"location string default \"haha\", " +
//				"p char default 'z', " +
//				"sex bool default true" +
//				");",
//			&ast.CreateTableStm{
//				TableName:  "tb_1",
//				IfNotExist: false,
//				Cols: []*ast.ColumnDefStm{
//					{ColName: "id", ColumnType: ast.ColumnType{Tp: lexer.INT}, PrimaryKey: true, ColDefaultValue: ast.ColumnValue{Value: 0, ValueType: lexer.INTVALUE}},
//					{ColName: "name", ColumnType: ast.ColumnType{Tp: lexer.VARCHAR, Min: 20}, ColDefaultValue: ast.ColumnValue{Value: "hello", ValueType: lexer.STRINGVALUE}},
//					{ColName: "age", ColumnType: ast.ColumnType{Tp: lexer.FLOAT, Min: 10, Max: 2}, ColDefaultValue: ast.ColumnValue{Value: 10, ValueType: lexer.INTVALUE}},
//					{ColName: "location", ColumnType: ast.ColumnType{Tp: lexer.STRING}, ColDefaultValue: ast.ColumnValue{Value: "haha", ValueType: lexer.STRINGVALUE}},
//					{ColName: "p", ColumnType: ast.ColumnType{Tp: lexer.CHAR}, ColDefaultValue: ast.ColumnValue{Value: byte('z'), ValueType: lexer.CHARVALUE}},
//					{ColName: "sex", ColumnType: ast.ColumnType{Tp: lexer.BOOL}, ColDefaultValue: ast.ColumnValue{Value: true, ValueType: lexer.TRUE}},
//				},
//			},
//		},
//		{
//			"create database db_1;",
//			&ast.CreateDatabaseStm{
//				DatabaseName: "db_1",
//				IfNotExist:   false,
//			},
//		},
//		{
//			"create database if not exist `db_1`;",
//			&ast.CreateDatabaseStm{
//				DatabaseName: "db_1",
//				IfNotExist:   true,
//			},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestDelete(t *testing.T) {
//	var whereExpre ast.ExpressionStm
//	and := ast.OperationStm(lexer.AND)
//	age := ast.ColumnRefStm("age")
//	whereExpre.Append(&age)
//	equalOp := ast.OperationStm(lexer.CHECKEQUAL)
//	whereExpre.Append(&equalOp)
//	whereExpre.Append(&ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 10})
//	whereExpre.Append(&and)
//	sex := ast.ColumnRefStm("sex")
//	whereExpre.Append(&sex)
//	whereExpre.Append(&equalOp)
//	whereExpre.Append(&ast.ColumnValue{ValueType: lexer.TRUE, Value: true})
//	id := ast.ColumnRefStm("id")
//	sqls := []testEntity{
//		{
//			"delete from tb_1 where age==10 AND sex==true;",
//			&ast.DeleteStm{TableName: "tb_1", WhereStm: &ast.WhereStm{ExpressionStms: &whereExpre}},
//		},
//		{
//			"delete from tb_1 where id == 1 order by sex, age limit 5;",
//			&ast.DeleteStm{
//				TableName:  "tb_1",
//				WhereStm:   &ast.WhereStm{ExpressionStms: &ast.ExpressionStm{Params: []ast.Stm{&id, &equalOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1}}}},
//				LimitStm:   &ast.LimitStm{Count: 5},
//				OrderByStm: &ast.OrderByStm{Cols: []string{"sex", "age"}}},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestDrop(t *testing.T) {
//	sqls := []testEntity{
//		{
//			"drop table if exist tb_1 	;",
//			&ast.DropTableStm{TableNames: []string{"tb_1"}, IfExist: true},
//		},
//		{
//			"drop table tb_1;",
//			&ast.DropTableStm{TableNames: []string{"tb_1"}, IfExist: false},
//		},
//		{
//			"drop database if exist db_2; ",
//			&ast.DropDatabaseStm{IfExist: true, DatabaseName: "db_2"},
//		},
//		{
//			"drop database db_2  	;",
//			&ast.DropDatabaseStm{IfExist: false, DatabaseName: "db_2"},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestInsert(t *testing.T) {
//	add := ast.OperationStm(lexer.ADD)
//	sqls := []testEntity{
//		{
//			"insert into tb_1 values (1 + 100 + sqrt(10.0), 20.5, 'z', \"hello\", true);",
//			&ast.InsertIntoStm{TableName: "tb_1",
//				ValueExpressions: []ast.Stm{
//					&ast.ExpressionStm{
//						Params: []ast.Stm{
//							&ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1},
//							&add,
//							&ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 100},
//							&add,
//							&ast.FunctionCallStm{Name: "sqrt", Params: []ast.Stm{&ast.ColumnValue{ValueType: lexer.FLOATVALUE, Value: 10.0}}},
//						}},
//					&ast.ColumnValue{ValueType: lexer.FLOATVALUE, Value: 20.5},
//					&ast.ColumnValue{ValueType: lexer.CHARVALUE, Value: byte('z')},
//					&ast.ColumnValue{ValueType: lexer.STRINGVALUE, Value: "hello"},
//					&ast.ColumnValue{ValueType: lexer.TRUE, Value: true},
//				}},
//		},
//		{
//			"insert into tb_1(name1, `col2`) values(1, \"hello\");",
//			&ast.InsertIntoStm{TableName: "tb_1", Cols: []string{"name1", "col2"},
//				ValueExpressions: []ast.Stm{
//					&ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1},
//					&ast.ColumnValue{ValueType: lexer.STRINGVALUE, Value: "hello"},
//				}},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestRename(t *testing.T) {
//	sqls := []testEntity{
//		{
//			"rename table tb_1 to tb_2;",
//			&ast.RenameStm{Tp: lexer.TABLE, OrigNames: "tb_1", ModifiedNames: "tb_2"},
//		},
//		{
//			"rename database tb_1 to tb_2;",
//			&ast.RenameStm{Tp: lexer.DATABASE, OrigNames: "tb_1", ModifiedNames: "tb_2"},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestSelect(t *testing.T) {
//	var nilWhereStm *ast.WhereStm
//	var nilOrderbyStm *ast.OrderByStm
//	var nilLimitStm *ast.LimitStm
//	id := ast.ColumnRefStm("id")
//	name := ast.ColumnRefStm("name")
//	count := ast.ColumnRefStm("count")
//	age := ast.ColumnRefStm("age")
//	equalOp := ast.OperationStm(lexer.CHECKEQUAL)
//	addOp := ast.OperationStm(lexer.ADD)
//	sqls := []testEntity{
//		{
//			"select * from tb_1;",
//			&ast.SelectStm{
//				TableName:  "tb_1",
//				WhereStm:   nilWhereStm,
//				OrderByStm: nilOrderbyStm,
//				LimitStm:   nilLimitStm,
//			},
//		},
//		{
//			"select * from tb_1 where id == 1 order by name limit 10;",
//			&ast.SelectStm{
//				TableName:  "tb_1",
//				WhereStm:   &ast.WhereStm{ExpressionStms: &ast.ExpressionStm{Params: []ast.Stm{&id, &equalOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1}}}},
//				OrderByStm: &ast.OrderByStm{Cols: []string{"name"}},
//				LimitStm:   &ast.LimitStm{Count: 10},
//			},
//		},
//		{
//			"select name+5, age + 4, count from tb_1 where id==1 order by name limit 1;",
//			&ast.SelectStm{
//				Expressions: []ast.Stm{
//					&ast.ExpressionStm{Params: []ast.Stm{&name, &addOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 5}}},
//					&ast.ExpressionStm{Params: []ast.Stm{&age, &addOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 4}}},
//					&count,
//				},
//				TableName:  "tb_1",
//				WhereStm:   &ast.WhereStm{ExpressionStms: &ast.ExpressionStm{Params: []ast.Stm{&id, &equalOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1}}}},
//				OrderByStm: &ast.OrderByStm{Cols: []string{"name"}},
//				LimitStm:   &ast.LimitStm{Count: 1},
//			},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestTruncate(t *testing.T) {
//	sqls := []testEntity{
//		{
//			"truncate table tb_1;",
//			&ast.TruncateStm{TableName: "tb_1"},
//		},
//		{
//			"truncate `tb_1`;",
//			&ast.TruncateStm{TableName: "tb_1"},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestUpdate(t *testing.T) {
//	id := ast.ColumnRefStm("id")
//	name := ast.ColumnRefStm("name")
//	c := ast.ColumnRefStm("c")
//	b := ast.ColumnRefStm("b")
//	f := ast.ColumnRefStm("f")
//	age := ast.ColumnRefStm("age")
//	andOp := ast.OperationStm(lexer.AND)
//	equalOp := ast.OperationStm(lexer.ASSIGNEQUAL)
//	checkEqualOp := ast.OperationStm(lexer.CHECKEQUAL)
//	sqls := []testEntity{
//		{
//			"update tb_1 set id=1, name=\"hello\", c='x', b=false, f=10.5;",
//			&ast.UpdateStm{
//				TableName: "tb_1",
//				Expressions: []ast.Stm{
//					&ast.ExpressionStm{Params: []ast.Stm{&id, &equalOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1}}},
//					&ast.ExpressionStm{Params: []ast.Stm{&name, &equalOp, &ast.ColumnValue{ValueType: lexer.STRINGVALUE, Value: "hello"}}},
//					&ast.ExpressionStm{Params: []ast.Stm{&c, &equalOp, &ast.ColumnValue{ValueType: lexer.CHARVALUE, Value: byte('x')}}},
//					&ast.ExpressionStm{Params: []ast.Stm{&b, &equalOp, &ast.ColumnValue{ValueType: lexer.FALSE, Value: false}}},
//					&ast.ExpressionStm{Params: []ast.Stm{&f, &equalOp, &ast.ColumnValue{ValueType: lexer.FLOATVALUE, Value: 10.5}}},
//				},
//			},
//		},
//		{
//			"update tb_1 set id=1 where age==10 and id==1;",
//			&ast.UpdateStm{
//				TableName: "tb_1",
//				Expressions: []ast.Stm{
//					&ast.ExpressionStm{Params: []ast.Stm{&id, &equalOp, &ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1}}},
//				},
//				WhereStm: &ast.WhereStm{
//					ExpressionStms: &ast.ExpressionStm{
//						Params: []ast.Stm{
//							&age,
//							&checkEqualOp,
//							&ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 10},
//							&andOp,
//							&id,
//							&checkEqualOp,
//							&ast.ColumnValue{ValueType: lexer.INTVALUE, Value: 1},
//						},
//					},
//				},
//			},
//		},
//	}
//	testSqls(t, sqls)
//}
//
//func TestAlter(t *testing.T) {
//	sqls := []testEntity{
//		{
//			"alter table tb_1 drop column col1;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.DROP,
//				ColDef: &ast.ColumnDefStm{
//					OldColName: "",
//					ColName:    "col1",
//				},
//			},
//		},
//		{
//			"alter table tb_2 drop col2		;",
//			&ast.AlterStm{
//				TableName: "tb_2",
//				Tp:        lexer.DROP,
//				ColDef: &ast.ColumnDefStm{
//					OldColName: "",
//					ColName:    "col2",
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id int primary key;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.COLADD,
//				ColDef: &ast.ColumnDefStm{
//					ColName:    "id",
//					PrimaryKey: true,
//					ColumnType: ast.ColumnType{Tp: lexer.INT},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id varchar(10) default \"hello\";",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.COLADD,
//				ColDef: &ast.ColumnDefStm{
//					ColName:         "id",
//					ColDefaultValue: ast.ColumnValue{ValueType: lexer.STRINGVALUE, Value: "hello"},
//					ColumnType:      ast.ColumnType{Tp: lexer.VARCHAR, Min: 10},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id float(10, 2) default 10.5;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.COLADD,
//				ColDef: &ast.ColumnDefStm{
//					ColName:         "id",
//					ColDefaultValue: ast.ColumnValue{ValueType: lexer.FLOATVALUE, Value: 10.5},
//					ColumnType:      ast.ColumnType{Tp: lexer.FLOAT, Min: 10, Max: 2},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id2 float default 10.5;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.COLADD,
//				ColDef: &ast.ColumnDefStm{
//					ColName:         "id2",
//					ColDefaultValue: ast.ColumnValue{ValueType: lexer.FLOATVALUE, Value: 10.5},
//					ColumnType:      ast.ColumnType{Tp: lexer.FLOAT, Min: -1, Max: -1},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id3 char default 'z' primary key ;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.COLADD,
//				ColDef: &ast.ColumnDefStm{
//					ColName:         "id3",
//					ColDefaultValue: ast.ColumnValue{Value: byte('z'), ValueType: lexer.CHARVALUE},
//					PrimaryKey:      true,
//					ColumnType:      ast.ColumnType{Tp: lexer.CHAR},
//				},
//			},
//		},
//		{
//			"alter table tb_1 alter column col3 char default 'z' primary key  ;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.ALTER,
//				ColDef: &ast.ColumnDefStm{
//					ColName:         "col3",
//					ColDefaultValue: ast.ColumnValue{ValueType: lexer.CHARVALUE, Value: byte('z')},
//					PrimaryKey:      true,
//					ColumnType:      ast.ColumnType{Tp: lexer.CHAR},
//				},
//			},
//		},
//		{
//			"alter table tb_1 change column col_o col_3 float default 10.5 Primary key;",
//			&ast.AlterStm{
//				TableName: "tb_1",
//				Tp:        lexer.CHANGE,
//				ColDef: &ast.ColumnDefStm{
//					OldColName:      "col_o",
//					ColName:         "col_3",
//					ColDefaultValue: ast.ColumnValue{ValueType: lexer.FLOATVALUE, Value: 10.5},
//					PrimaryKey:      true,
//					ColumnType:      ast.ColumnType{Tp: lexer.FLOAT, Min: -1, Max: -1},
//				},
//			},
//		},
//	}
//	testSqls(t, sqls)
//}
