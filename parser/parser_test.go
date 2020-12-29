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
		assert.Nil(t, err, sql)
		assert.Equal(t, sql.stm, stm, sql)
	}
}

func TestCreateStm(t *testing.T) {
	sqls := []testEntity{
		{
			"create table IF NOT EXIST tb_1" +
				"(" +
				"id int default 10 primary key, " +
				"name varchar( 20) default \"hello\", " +
				"age float(10, 2) default 10.5, " +
				"location text default \"haha\", " +
				"age char default 'z'," +
				"sex bool default true" +
				");",
			&CreateTableStm{
				TableName:  "tb_1",
				IfNotExist: true,
				Cols: []*ColumnDefStm{
					{ColName: "id", ColumnType: ColumnType{Tp: INT}, PrimaryKey: true, ColDefaultValue: ColumnValue([]byte("10"))},
					{ColName: "name", ColumnType: ColumnType{Tp: VARCHAR, Ranges: [2]int{20, 0}}, ColDefaultValue: ColumnValue([]byte("\"hello\""))},
					{ColName: "age", ColumnType: ColumnType{Tp: FLOAT, Ranges: [2]int{10, 2}}, ColDefaultValue: ColumnValue([]byte("10.5"))},
					{ColName: "location", ColumnType: ColumnType{Tp: TEXT}, ColDefaultValue: ColumnValue([]byte("\"haha\""))},
					{ColName: "age", ColumnType: ColumnType{Tp: CHAR}, ColDefaultValue: ColumnValue([]byte("'z'"))},
					{ColName: "sex", ColumnType: ColumnType{Tp: BOOL}, ColDefaultValue: ColumnValue([]byte("true"))},
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
	sqls := []testEntity{
		{
			"insert into tb_1 values (1 + 100 + sqrt(10.0), 20.5, 'z', \"hello\", true);",
			&InsertIntoStm{
				TableName: "tb_1",
				Values: []*ExpressionStm{
					{
						LeftExpr: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("1"))),
							},
							Op: OperationAdd,
							RightExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("100"))),
							},
						},
						Op: OperationAdd,
						RightExpr: &ExpressionTerm{
							UnaryOp: NoneUnaryOpTp,
							Tp:      FuncCallExpressionTermTP,
							RealExprTerm: FunctionCallExpressionStm{
								FuncName: "sqrt",
								Params: []*ExpressionStm{
									{
										LeftExpr: &ExpressionTerm{
											UnaryOp:      NoneUnaryOpTp,
											Tp:           LiteralExpressionTermTP,
											RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("10.0"))),
										},
									},
								},
							},
						},
					},
					{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("20.5"))),
						},
					},
					{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("'z'"))),
						},
					},
					{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("\"hello\""))),
						},
					},
					{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("true"))),
						},
					},
				},
			},
		},
		{
			"insert into tb_1(name1, `col2`) values(1, \"hello\");",
			&InsertIntoStm{
				TableName: "tb_1",
				Cols:      []string{"name1", "col2"},
				Values: []*ExpressionStm{
					{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("1"))),
						},
					},
					{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("\"hello\""))),
						},
					},
				}},
		},
	}
	testSqls(t, sqls)
}

func TestDeleteStm(t *testing.T) {
	sqls := []testEntity{
		{
			"delete from tb_1 where age=10 AND sex=true;",
			&SingleDeleteStm{
				TableRef: TableReferenceStm{
					Tp: TableReferenceTableFactorTp,
					TableReference: TableReferenceTableFactorStm{
						Tp: TableReferencePureTableNameTp,
						TableFactorReference: TableReferencePureTableRefStm{
							TableName: "tb_1",
						},
					},
				},
				Where: WhereStm(&ExpressionStm{
					LeftExpr: &ExpressionStm{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           IdentifierExpressionTermTP,
							RealExprTerm: IdentifierExpression([]byte("age")),
						},
						Op: OperationEqual,
						RightExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("10"))),
						},
					},
					Op: OperationAnd,
					RightExpr: &ExpressionStm{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           IdentifierExpressionTermTP,
							RealExprTerm: IdentifierExpression([]byte("sex")),
						},
						Op: OperationEqual,
						RightExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("true"))),
						},
					},
				}),
			},
		},
		{
			"delete from tb_1 where id > 1 order by sex, age limit 5;",
			&SingleDeleteStm{
				TableRef: TableReferenceStm{
					Tp: TableReferenceTableFactorTp,
					TableReference: TableReferenceTableFactorStm{
						Tp: TableReferencePureTableNameTp,
						TableFactorReference: TableReferencePureTableRefStm{
							TableName: "tb_1",
						},
					},
				},
				Where: WhereStm(&ExpressionStm{
					LeftExpr: &ExpressionTerm{
						UnaryOp:      NoneUnaryOpTp,
						Tp:           IdentifierExpressionTermTP,
						RealExprTerm: IdentifierExpression([]byte("id")),
					},
					Op: OperationGreat,
					RightExpr: &ExpressionTerm{
						UnaryOp:      NoneUnaryOpTp,
						Tp:           LiteralExpressionTermTP,
						RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("1"))),
					},
				}),
				Limit: &LimitStm{Count: 5},
				OrderBy: &OrderByStm{
					Expressions: []*OrderedExpressionStm{
						{
							Expression: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("sex")),
								},
							},
							Asc: true,
						},
						{
							Expression: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("age")),
								},
							},
							Asc: true,
						},
					},
				},
			},
		},
	}
	testSqls(t, sqls)
}

func TestSelectStm(t *testing.T) {
	sqls := []testEntity{
		{
			"select * from tb_1;",
			&SelectStm{
				Tp: SelectAllTp,
				SelectExpressions: &SelectExpressionStm{
					Tp: StarSelectExpressionTp,
				},
				TableReferences: []TableReferenceStm{
					{
						Tp: TableReferenceTableFactorTp,
						TableReference: TableReferenceTableFactorStm{
							Tp: TableReferencePureTableNameTp,
							TableFactorReference: TableReferencePureTableRefStm{
								TableName: "tb_1",
							},
						},
					},
				},
			},
		},
		{
			"select * from tb_1 where id <= 1 order by name limit 10;",
			&SelectStm{
				Tp: SelectAllTp,
				TableReferences: []TableReferenceStm{
					{
						Tp: TableReferenceTableFactorTp,
						TableReference: TableReferenceTableFactorStm{
							Tp: TableReferencePureTableNameTp,
							TableFactorReference: TableReferencePureTableRefStm{
								TableName: "tb_1",
							},
						},
					},
				},
				SelectExpressions: &SelectExpressionStm{
					Tp: StarSelectExpressionTp,
				},
				Where: WhereStm(&ExpressionStm{
					LeftExpr: &ExpressionTerm{
						UnaryOp:      NoneUnaryOpTp,
						Tp:           IdentifierExpressionTermTP,
						RealExprTerm: IdentifierExpression([]byte("id")),
					},
					Op: OperationLessEqual,
					RightExpr: &ExpressionTerm{
						UnaryOp:      NoneUnaryOpTp,
						Tp:           LiteralExpressionTermTP,
						RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("1"))),
					},
				}),
				OrderBy: &OrderByStm{
					Expressions: []*OrderedExpressionStm{
						{
							Expression: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("name")),
								},
							},
							Asc: true,
						},
					},
				},
				LimitStm: &LimitStm{Count: 10},
			},
		},
		{
			"select name+5 as name1, age * 4, count from tb_1 where id=1 order by name desc limit 1;",
			&SelectStm{
				Tp: SelectAllTp,
				SelectExpressions: &SelectExpressionStm{
					Tp: ExprSelectExpressionTp,
					Expr: []*SelectExpr{
						{
							Expr: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("name")),
								},
								Op: OperationAdd,
								RightExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           LiteralExpressionTermTP,
									RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("5"))),
								},
							},
							Alias: "name1",
						},
						{
							Expr: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("age")),
								},
								Op: OperationMul,
								RightExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           LiteralExpressionTermTP,
									RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("4"))),
								},
							},
						},
						{
							Expr: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("count")),
								},
							},
						},
					},
				},
				TableReferences: []TableReferenceStm{
					{
						Tp: TableReferenceTableFactorTp,
						TableReference: TableReferenceTableFactorStm{
							Tp: TableReferencePureTableNameTp,
							TableFactorReference: TableReferencePureTableRefStm{
								TableName: "tb_1",
							},
						},
					},
				},
				Where: WhereStm(&ExpressionStm{
					LeftExpr: &ExpressionTerm{
						UnaryOp:      NoneUnaryOpTp,
						Tp:           IdentifierExpressionTermTP,
						RealExprTerm: IdentifierExpression([]byte("id")),
					},
					Op: OperationEqual,
					RightExpr: &ExpressionTerm{
						UnaryOp:      NoneUnaryOpTp,
						Tp:           LiteralExpressionTermTP,
						RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("1"))),
					},
				}),
				OrderBy: &OrderByStm{
					Expressions: []*OrderedExpressionStm{
						{
							Expression: &ExpressionStm{
								LeftExpr: &ExpressionTerm{
									UnaryOp:      NoneUnaryOpTp,
									Tp:           IdentifierExpressionTermTP,
									RealExprTerm: IdentifierExpression([]byte("name")),
								},
							},
							Asc: false,
						},
					},
				},
				LimitStm: &LimitStm{Count: 1},
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
	sqls := []testEntity{
		{
			"update tb_1 set id=1, name='hello', c='x', b=false, f=10.5;",
			&UpdateStm{
				TableRefs: TableReferenceStm{
					Tp: TableReferenceTableFactorTp,
					TableReference: TableReferenceTableFactorStm{
						Tp: TableReferencePureTableNameTp,
						TableFactorReference: TableReferencePureTableRefStm{
							TableName: "tb_1",
						},
					},
				},
				Assignments: []*AssignmentStm{
					{
						ColName: "id",
						Value: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm([]byte("1")),
							},
						},
					},
					{
						ColName: "name",
						Value: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm([]byte("'hello'")),
							},
						},
					},
					{
						ColName: "c",
						Value: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm([]byte("'x'")),
							},
						},
					},
					{
						ColName: "b",
						Value: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm([]byte("false")),
							},
						},
					},
					{
						ColName: "f",
						Value: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm([]byte("10.5")),
							},
						},
					},
				},
			},
		},
		{
			"update tb_1 set id=id / 2 where age <= 10 or id > 1;",
			&UpdateStm{
				TableRefs: TableReferenceStm{
					Tp: TableReferenceTableFactorTp,
					TableReference: TableReferenceTableFactorStm{
						Tp: TableReferencePureTableNameTp,
						TableFactorReference: TableReferencePureTableRefStm{
							TableName: "tb_1",
						},
					},
				},
				Assignments: []*AssignmentStm{
					{
						ColName: "id",
						Value: &ExpressionStm{
							LeftExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           IdentifierExpressionTermTP,
								RealExprTerm: IdentifierExpression([]byte("id")),
							},
							Op: OperationDivide,
							RightExpr: &ExpressionTerm{
								UnaryOp:      NoneUnaryOpTp,
								Tp:           LiteralExpressionTermTP,
								RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("2"))),
							},
						},
					},
				},
				Where: WhereStm(&ExpressionStm{
					LeftExpr: &ExpressionStm{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           IdentifierExpressionTermTP,
							RealExprTerm: IdentifierExpression([]byte("age")),
						},
						Op: OperationLessEqual,
						RightExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("10"))),
						},
					},
					Op: OperationOr,
					RightExpr: &ExpressionStm{
						LeftExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           IdentifierExpressionTermTP,
							RealExprTerm: IdentifierExpression([]byte("id")),
						},
						Op: OperationGreat,
						RightExpr: &ExpressionTerm{
							UnaryOp:      NoneUnaryOpTp,
							Tp:           LiteralExpressionTermTP,
							RealExprTerm: LiteralExpressionStm(ColumnValue([]byte("1"))),
						},
					},
				}),
			},
		},
	}
	testSqls(t, sqls)
}

func TestUseStm(t *testing.T) {
	sql := "use test;"
	parser := NewParser()
	_, err := parser.Parse([]byte(sql))
	assert.Nil(t, err)
}

func TestShowStm(t *testing.T) {
	sql := "show tables;"
	parser := NewParser()
	_, err := parser.Parse([]byte(sql))
	assert.Nil(t, err)
	sql = "show databases;"
	_, err = parser.Parse([]byte(sql))
	assert.Nil(t, err)
}

//func TestAlterStm(t *testing.T) {
//	sqls := []testEntity{
//		{
//			"alter table tb_1 drop column col1;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       DROP,
//				ColDef: &ColumnDefStm{
//					OldColName: "",
//					ColName:    "col1",
//				},
//			},
//		},
//		{
//			"alter table tb_2 drop col2		;",
//			&AlterStm{
//				TableRef: "tb_2",
//				Tp:       DROP,
//				ColDef: &ColumnDefStm{
//					OldColName: "",
//					ColName:    "col2",
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id int primary key;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       COLADD,
//				ColDef: &ColumnDefStm{
//					ColName:    "id",
//					PrimaryKey: true,
//					ColumnType: ColumnType{Tp: INT},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id varchar(10) default \"hello\";",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       COLADD,
//				ColDef: &ColumnDefStm{
//					ColName:         "id",
//					ColDefaultValue: ColumnValue{ValueType: STRINGVALUE, Value: "hello"},
//					ColumnType:      ColumnType{Tp: VARCHAR, Min: 10},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id float(10, 2) default 10.5;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       COLADD,
//				ColDef: &ColumnDefStm{
//					ColName:         "id",
//					ColDefaultValue: ColumnValue{ValueType: FLOATVALUE, Value: 10.5},
//					ColumnType:      ColumnType{Tp: FLOAT, Min: 10, Max: 2},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id2 float default 10.5;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       COLADD,
//				ColDef: &ColumnDefStm{
//					ColName:         "id2",
//					ColDefaultValue: ColumnValue{ValueType: FLOATVALUE, Value: 10.5},
//					ColumnType:      ColumnType{Tp: FLOAT, Min: -1, Max: -1},
//				},
//			},
//		},
//		{
//			"alter table tb_1 add column id3 char default 'z' primary key ;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       COLADD,
//				ColDef: &ColumnDefStm{
//					ColName:         "id3",
//					ColDefaultValue: ColumnValue{Value: byte('z'), ValueType: CHARVALUE},
//					PrimaryKey:      true,
//					ColumnType:      ColumnType{Tp: CHAR},
//				},
//			},
//		},
//		{
//			"alter table tb_1 alter column col3 char default 'z' primary key  ;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       ALTER,
//				ColDef: &ColumnDefStm{
//					ColName:         "col3",
//					ColDefaultValue: ColumnValue{ValueType: CHARVALUE, Value: byte('z')},
//					PrimaryKey:      true,
//					ColumnType:      ColumnType{Tp: CHAR},
//				},
//			},
//		},
//		{
//			"alter table tb_1 change column col_o col_3 float default 10.5 Primary key;",
//			&AlterStm{
//				TableRef: "tb_1",
//				Tp:       CHANGE,
//				ColDef: &ColumnDefStm{
//					OldColName:      "col_o",
//					ColName:         "col_3",
//					ColDefaultValue: ColumnValue{ValueType: FLOATVALUE, Value: 10.5},
//					PrimaryKey:      true,
//					ColumnType:      ColumnType{Tp: FLOAT, Min: -1, Max: -1},
//				},
//			},
//		},
//	}
//	testSqls(t, sqls)
//}
