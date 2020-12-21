package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testEntity struct {
	sql string
	stm Stm
}

func testSqls(t *testing.T, sqls []testEntity) {
	parser := NewParser()
	for _, sql := range sqls {
		stm, err := parser.Parse([]byte(sql.sql))
		//log.LogDebug("%+v\n", stm)
		assert.Nil(t, err, sql)
		assert.Equal(t, sql.stm, stm, sql)
	}
}

func TestCreateStm(t *testing.T) {
	sqls := []testEntity{
		{
			"create table IF NOT EXIST tb_1" +
				"(" +
				"id int default 0 primary key, " +
				"name varchar( 20) default \"hello\", " +
				"age float(10, 2) default 10, " +
				"location text default \"haha\", " +
				"age char default 'z'," +
				"sex bool default true" +
				");",
			&CreateTableStm{
				TableName:  "tb_1",
				IfNotExist: true,
				Cols: []*ColumnDefStm{
					{ColName: "id", ColumnType: ColumnType{Tp: INT}, PrimaryKey: true, ColDefaultValue: ColumnValue([]byte("0"))},
					{ColName: "name", ColumnType: ColumnType{Tp: VARCHAR, Ranges: [2]int{0, 20}}, ColDefaultValue: ColumnValue([]byte("hello"))},
					{ColName: "age", ColumnType: ColumnType{Tp: FLOAT, Ranges: [2]int{10, 2}}, ColDefaultValue: ColumnValue([]byte("10"))},
					{ColName: "location", ColumnType: ColumnType{Tp: TEXT}, ColDefaultValue: ColumnValue([]byte("haha"))},
					{ColName: "age", ColumnType: ColumnType{Tp: CHAR}, ColDefaultValue: ColumnValue([]byte{'z'})},
				},
			},
		},
		{
			"create database db_1;",
			&CreateDatabaseStm{
				DatabaseName: "db_1",
				IfNotExist:   false,
			},
		},
		{
			"create database if not exist `db_1`;",
			&CreateDatabaseStm{
				DatabaseName: "db_1",
				IfNotExist:   true,
			},
		},
	}
	testSqls(t, sqls)
}

func TestDropStm(t *testing.T) {
	sqls := []testEntity{
		{
			"drop table if exist tb_1 	;",
			&DropTableStm{TableNames: []string{"tb_1"}, IfExists: true},
		},
		{
			"drop table tb_1;",
			&DropTableStm{TableNames: []string{"tb_1"}, IfExists: false},
		},
		{
			"drop database if exist db_2; ",
			&DropDatabaseStm{IfExist: true, DatabaseName: "db_2"},
		},
		{
			"drop database db_2  	;",
			&DropDatabaseStm{IfExist: false, DatabaseName: "db_2"},
		},
	}
	testSqls(t, sqls)
}

func TestRenameStm(t *testing.T) {
	sqls := []testEntity{
		{
			"rename table tb_1 to tb_2;",
			&RenameStm{OrigNames: []string{"tb_1"}, ModifiedNames: []string{"tb_2"}},
		},
	}
	testSqls(t, sqls)
}

func TestInsertStm(t *testing.T) {
	add := OperationStm(ADD)
	sqls := []testEntity{
		{
			"insert into tb_1 values (1 + 100 + sqrt(10.0), 20.5, 'z', \"hello\", true);",
			&InsertIntoStm{TableRef: "tb_1",
				ValueExpressions: []Stm{
					&ExpressionStm{
						Params: []Stm{
							&ColumnValue{ValueType: INTVALUE, Value: 1},
							&add,
							&ColumnValue{ValueType: INTVALUE, Value: 100},
							&add,
							&FunctionCallStm{Name: "sqrt", Params: []Stm{&ColumnValue{ValueType: FLOATVALUE, Value: 10.0}}},
						}},
					&ColumnValue{ValueType: FLOATVALUE, Value: 20.5},
					&ColumnValue{ValueType: CHARVALUE, Value: byte('z')},
					&ColumnValue{ValueType: STRINGVALUE, Value: "hello"},
					&ColumnValue{ValueType: TRUE, Value: true},
				}},
		},
		{
			"insert into tb_1(name1, `col2`) values(1, \"hello\");",
			&InsertIntoStm{TableRef: "tb_1", Cols: []string{"name1", "col2"},
				ValueExpressions: []Stm{
					&ColumnValue{ValueType: INTVALUE, Value: 1},
					&ColumnValue{ValueType: STRINGVALUE, Value: "hello"},
				}},
		},
	}
	testSqls(t, sqls)
}

func TestDeleteStm(t *testing.T) {
	var whereExpre ExpressionStm
	sqls := []testEntity{
		{
			"delete from tb_1 where age==10 AND sex==true;",
			&SingleDeleteStm{TableRef: TableReferenceStm{}, Where: &WhereStm{ExpressionStms: &whereExpre}},
		},
		{
			"delete from tb_1 where id == 1 order by sex, age limit 5;",
			&SingleDeleteStm{
				TableRef: "tb_1",
				Where:    &WhereStm{ExpressionStms: &ExpressionStm{Params: []Stm{&id, &equalOp, &ColumnValue{ValueType: INTVALUE, Value: 1}}}},
				Limit:    &LimitStm{Count: 5},
				OrderBy:  &OrderByStm{Cols: []string{"sex", "age"}}},
		},
	}
	testSqls(t, sqls)
}

func TestSelectStm(t *testing.T) {
	var nilWhereStm *WhereStm
	var nilOrderbyStm *OrderByStm
	var nilLimitStm *LimitStm
	id := ColumnRefStm("id")
	name := ColumnRefStm("name")
	count := ColumnRefStm("count")
	age := ColumnRefStm("age")
	equalOp := OperationStm(CHECKEQUAL)
	addOp := OperationStm(ADD)
	sqls := []testEntity{
		{
			"select * from tb_1;",
			&SelectStm{
				TableRef:   "tb_1",
				WhereStm:   nilWhereStm,
				OrderByStm: nilOrderbyStm,
				LimitStm:   nilLimitStm,
			},
		},
		{
			"select * from tb_1 where id == 1 order by name limit 10;",
			&SelectStm{
				TableRef:   "tb_1",
				WhereStm:   &WhereStm{ExpressionStms: &ExpressionStm{Params: []Stm{&id, &equalOp, &ColumnValue{ValueType: INTVALUE, Value: 1}}}},
				OrderByStm: &OrderByStm{Cols: []string{"name"}},
				LimitStm:   &LimitStm{Count: 10},
			},
		},
		{
			"select name+5, age + 4, count from tb_1 where id==1 order by name limit 1;",
			&SelectStm{
				Expressions: []Stm{
					&ExpressionStm{Params: []Stm{&name, &addOp, &ColumnValue{ValueType: INTVALUE, Value: 5}}},
					&ExpressionStm{Params: []Stm{&age, &addOp, &ColumnValue{ValueType: INTVALUE, Value: 4}}},
					&count,
				},
				TableRef:   "tb_1",
				WhereStm:   &WhereStm{ExpressionStms: &ExpressionStm{Params: []Stm{&id, &equalOp, &ColumnValue{ValueType: INTVALUE, Value: 1}}}},
				OrderByStm: &OrderByStm{Cols: []string{"name"}},
				LimitStm:   &LimitStm{Count: 1},
			},
		},
	}
	testSqls(t, sqls)
}

func TestTruncateStm(t *testing.T) {
	sqls := []testEntity{
		{
			"truncate table tb_1;",
			&TruncateStm{TableName: "tb_1"},
		},
		{
			"truncate `tb_1`;",
			&TruncateStm{TableName: "tb_1"},
		},
	}
	testSqls(t, sqls)
}

func TestUpdateStm(t *testing.T) {
	id := ColumnRefStm("id")
	name := ColumnRefStm("name")
	c := ColumnRefStm("c")
	b := ColumnRefStm("b")
	f := ColumnRefStm("f")
	age := ColumnRefStm("age")
	andOp := OperationStm(AND)
	equalOp := OperationStm(ASSIGNEQUAL)
	checkEqualOp := OperationStm(CHECKEQUAL)
	sqls := []testEntity{
		{
			"update tb_1 set id=1, name=\"hello\", c='x', b=false, f=10.5;",
			&UpdateStm{
				TableRef: "tb_1",
				Expressions: []Stm{
					&ExpressionStm{Params: []Stm{&id, &equalOp, &ColumnValue{ValueType: INTVALUE, Value: 1}}},
					&ExpressionStm{Params: []Stm{&name, &equalOp, &ColumnValue{ValueType: STRINGVALUE, Value: "hello"}}},
					&ExpressionStm{Params: []Stm{&c, &equalOp, &ColumnValue{ValueType: CHARVALUE, Value: byte('x')}}},
					&ExpressionStm{Params: []Stm{&b, &equalOp, &ColumnValue{ValueType: FALSE, Value: false}}},
					&ExpressionStm{Params: []Stm{&f, &equalOp, &ColumnValue{ValueType: FLOATVALUE, Value: 10.5}}},
				},
			},
		},
		{
			"update tb_1 set id=1 where age==10 and id==1;",
			&UpdateStm{
				TableRef: "tb_1",
				Expressions: []Stm{
					&ExpressionStm{Params: []Stm{&id, &equalOp, &ColumnValue{ValueType: INTVALUE, Value: 1}}},
				},
				WhereStm: &WhereStm{
					ExpressionStms: &ExpressionStm{
						Params: []Stm{
							&age,
							&checkEqualOp,
							&ColumnValue{ValueType: INTVALUE, Value: 10},
							&andOp,
							&id,
							&checkEqualOp,
							&ColumnValue{ValueType: INTVALUE, Value: 1},
						},
					},
				},
			},
		},
	}
	testSqls(t, sqls)
}

func TestAlterStm(t *testing.T) {
	sqls := []testEntity{
		{
			"alter table tb_1 drop column col1;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       DROP,
				ColDef: &ColumnDefStm{
					OldColName: "",
					ColName:    "col1",
				},
			},
		},
		{
			"alter table tb_2 drop col2		;",
			&AlterStm{
				TableRef: "tb_2",
				Tp:       DROP,
				ColDef: &ColumnDefStm{
					OldColName: "",
					ColName:    "col2",
				},
			},
		},
		{
			"alter table tb_1 add column id int primary key;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       COLADD,
				ColDef: &ColumnDefStm{
					ColName:    "id",
					PrimaryKey: true,
					ColumnType: ColumnType{Tp: INT},
				},
			},
		},
		{
			"alter table tb_1 add column id varchar(10) default \"hello\";",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       COLADD,
				ColDef: &ColumnDefStm{
					ColName:         "id",
					ColDefaultValue: ColumnValue{ValueType: STRINGVALUE, Value: "hello"},
					ColumnType:      ColumnType{Tp: VARCHAR, Min: 10},
				},
			},
		},
		{
			"alter table tb_1 add column id float(10, 2) default 10.5;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       COLADD,
				ColDef: &ColumnDefStm{
					ColName:         "id",
					ColDefaultValue: ColumnValue{ValueType: FLOATVALUE, Value: 10.5},
					ColumnType:      ColumnType{Tp: FLOAT, Min: 10, Max: 2},
				},
			},
		},
		{
			"alter table tb_1 add column id2 float default 10.5;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       COLADD,
				ColDef: &ColumnDefStm{
					ColName:         "id2",
					ColDefaultValue: ColumnValue{ValueType: FLOATVALUE, Value: 10.5},
					ColumnType:      ColumnType{Tp: FLOAT, Min: -1, Max: -1},
				},
			},
		},
		{
			"alter table tb_1 add column id3 char default 'z' primary key ;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       COLADD,
				ColDef: &ColumnDefStm{
					ColName:         "id3",
					ColDefaultValue: ColumnValue{Value: byte('z'), ValueType: CHARVALUE},
					PrimaryKey:      true,
					ColumnType:      ColumnType{Tp: CHAR},
				},
			},
		},
		{
			"alter table tb_1 alter column col3 char default 'z' primary key  ;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       ALTER,
				ColDef: &ColumnDefStm{
					ColName:         "col3",
					ColDefaultValue: ColumnValue{ValueType: CHARVALUE, Value: byte('z')},
					PrimaryKey:      true,
					ColumnType:      ColumnType{Tp: CHAR},
				},
			},
		},
		{
			"alter table tb_1 change column col_o col_3 float default 10.5 Primary key;",
			&AlterStm{
				TableRef: "tb_1",
				Tp:       CHANGE,
				ColDef: &ColumnDefStm{
					OldColName:      "col_o",
					ColName:         "col_3",
					ColDefaultValue: ColumnValue{ValueType: FLOATVALUE, Value: 10.5},
					PrimaryKey:      true,
					ColumnType:      ColumnType{Tp: FLOAT, Min: -1, Max: -1},
				},
			},
		},
	}
	testSqls(t, sqls)
}
