package ast

import (
	"simpleDb/lexer"
)

type Stm interface {
	Execute() error
}

// create database statement:
// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];
type CreateDatabaseStm struct {
	DatabaseName string
	IfNotExist   bool
	Charset      string
	Collate      string
}

// create table statements:
// * create table [if not exist] tb_name like orig_tab_name;
// * create table [if not exist] tb_name2 (
//    Column_Def..., Index_Def..., Constraint_Def...
//    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];
// create table [if not exist] tb_name3 as selectStatement;

// * create table [if not exist] tb_name2 (
//    Column_Def..., Index_Def..., Constraint_Def...
//    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];
type CreateTableStm struct {
	TableName   string
	IfNotExist  bool
	Cols        []*ColumnDefStm
	Indexes     []IndexDefStm
	Constraints []ConstraintDefStm
	Engine      string
	Charset     string
	Collate     string
}

// create table [if not exist] as selectStatement;
type CreateTableAsSelectStm struct {
	TableName  string
	IfNotExist bool
	Select     *SelectStm
}

// * create table [if not exist] tb_name like orig_tab_name;
type CreateTableLikeStm struct {
	TableName      string
	IfNotExist     bool
	LikedTableName string
}

// columnDef:
// * col_name col_type [not null|null] [default default_value] [AUTO_INCREMENT] [[primary] key] [[unique] key]
type ColumnDefStm struct {
	ColName         string
	ColumnType      ColumnType
	AllowNULL       bool
	ColDefaultValue ColumnValue
	AutoIncrement   bool
	PrimaryKey      bool
	UniqueKey       bool
}

type ColumnValue []byte

// ColumnType represent a column type statement where tp is the column type and
// Ranges is the column type range, like int(10), 10 is range, float(10, 2), 10 and 2 is
// the ranges.
type ColumnType struct {
	Tp     lexer.TokenType
	Ranges [2]int
}

// Index_Def:
// * {index|key} index_name (col_name, ...)
type IndexDefStm struct {
	IndexName string
	ColNames  []string
}

type ConstraintDefStm struct {
	Tp         ConstraintTp
	Constraint interface{}
}

type ConstraintTp byte

const (
	PrimaryKeyConstraintTp ConstraintTp = iota
	UniqueKeyConstraintTp
	ForeignKeyConstraintTp
)

// * [Constraint] primary key (col_name [,col_name...)
// * [Constraint] unique {index|key} index_name (col_name [,col_name...)
// * [Constraint] foreign key index_name (col_name [,col_name...) references tb_name (key...)
// [on {delete|update}] reference_option]
// reference_option is like: {restrict | cascade | set null | no action | set default}
// Restrict is the default
type PrimaryKeyDefStm struct {
	ColNames []string
}

type UniqueKeyDefStm struct {
	IndexName string
	ColNames  []string
}

type ForeignKeyConstraintDefStm struct {
	IndexName       string
	Cols            []string
	RefTableName    string
	RefKeys         []string
	DeleteRefOption ReferenceOptionTp
	UpdateRefOption ReferenceOptionTp
}

type ReferenceOptionTp byte

const (
	RefOptionRestrict ReferenceOptionTp = iota
	RefOptionCascade
	RefOptionSetNull
	RefOptionNoAction
	RefOptionSetDefault
)

// Drop database statement is like:
// * drop {database | schema} [if exists] db_name;
type DropDatabaseStm struct {
	DatabaseName string
	IfExist      bool
}

// Drop table statement is like:
// * drop table [if exists] tb_name[,tb_name...];
type DropTableStm struct {
	IfExists   bool
	TableNames []string
}

// Rename statement can be rename table or database statement.
// It's like:
// * rename table {tb1 To tb2...}
type RenameStm struct {
	OrigNames     []string
	ModifiedNames []string
}

// Truncate table statement is like:
// * truncate [table] tb_name
type TruncateStm struct {
	TableName string
}

// Alter statement can be alter table statement or alter database statement.
// Alter table statement is like:
// * alter [table] tb_name [
// add 	  [column] col_def |
// drop   [column] col_name |
// modify [column] col_def |
// change [column] old_col_name col_def |
// add {index|key} indexDef |
// add [constraint] primaryKeyDef |
// add [constraint] uniqueKeyDef |
// add [constraint] foreignKeyDef |
// drop {index|key} index_name |
// drop primary key |
// drop foreign key key_name |
// engine=value |
// [[default] | character set = value] |
// [[default] | collate = value]
// ]

// * alter [table] tb_name [
// add 	  [column] col_def |
// drop   [column] col_name |
// modify [column] col_def |
// change [column] old_col_name col_def |
type AlterTableAlterColumnStm struct {
	TableName string
	Tp        AlterTableColumnTp
	ColName   string
	ColDef    *ColumnDefStm
}

type AlterTableColumnTp byte

const (
	AddColumnTp AlterTableColumnTp = iota
	DropColumnTp
	ModifyColumnTp
	ChangeColumnTp
)

// Alter table statement is like:
// alter [table] tb_name
// * add {index|key} indexDef |
// * add [constraint] primaryKeyDef |
// * add [constraint] uniqueKeyDef |
// * add [constraint] foreignKeyDef |
// * drop {index|key} index_name |
// * drop primary key |
// * drop foreign key key_name |
type AlterTableDropIndexOrConstraintStm struct {
	TableName      string
	Tp             KeyOrConstraintType
	IndexOrKeyName string
}

type KeyOrConstraintType byte

const (
	IndexTp = iota
	PrimaryKeyTp
	ForeignKeyTp
)

// * add {index|key} indexDef |
// * add [constraint] primaryKeyDef |
// * add [constraint] uniqueKeyDef |
// * add [constraint] foreignKeyDef |
type AlterTableAddIndexOrConstraintStm struct {
	Tp                IndexOrConstraintTp
	TableName         string
	IndexOrConstraint interface{}
}

type IndexOrConstraintTp byte

const (
	IsIndexTp IndexOrConstraintTp = iota
	IsConstraintTp
)

// alter [table] tb_name
// [[default] | character set = value] |
// [[default] | collate = value]
type AlterTableCharsetCollateStm struct {
	TableName string
	Charset   string
	Collate   string
}

type AlterTableAlterEngineStm struct {
	TableName string
	Engine    string
}

// Alter database statement can be:
// * alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]
type AlterDatabaseStm struct {
	DatabaseName string
	Charset      string
	Collate      string
}

// Second DML
// Insert statement is like:
// * insert into tb_name [( col_name,... )] values (expression,...)
type InsertIntoStm struct {
	TableName string
	Cols      []string
	Values    []*ExpressionStm
}

type OperationStm lexer.TokenType

// For expression, compared to mysql, we use a simplified version and only a subset expressions of mysql
// are supported. An expression statement is like:
// term (ope term)
// a term can be:
// * literal | (expr) | identifier | functionCall |
// where functionCall is like:
// funcName(expr,...)
// where ope supports:
// +, -, *, /, %, =, IS, !=, IS NOT, >, >=, <, <=, AND, OR,
// Note: currently we don't consider [NOT] IN, [NOT] LIKE
type ExpressionStm struct {
	ExprTerms []ExpressionTerm
	Ops       []ExpressionOpTP
}

type ExpressionTerm struct {
	Tp           ExpressionTermTP
	RealExprTerm interface{}
}

type ExpressionTermTP byte

const (
	LiteralExpressionTermTP ExpressionTermTP = 0
	SubExpressionTermTP
	IdentifierExpressionTermTP
	FuncCallExpressionTermTP
)

type ExpressionOpTP lexer.TokenType

const (
	OperationAddTp        = lexer.ADD
	OperationMinusTp      = lexer.MINUS
	OperationMulTp        = lexer.MUL
	OperationDivideTp     = lexer.DIVIDE
	OperationModTp        = lexer.MOD
	OperationEqualTp      = lexer.EQUAL
	OperationIsTp         = lexer.IS
	OperationNotEqualTp   = lexer.NOTEQUAL
	OperationGreatTp      = lexer.GREAT
	OperationGreatEqualTp = lexer.GREATEQUAL
	OperationLessTp       = lexer.LESS
	OperationLessEqualTp  = lexer.LESSEQUAL
	OperationAndTp        = lexer.AND
	OperationOrTp         = lexer.OR
	OperationISNotTp      = lexer.OR + 1
)

//type ExpressionInExpressionsStm struct {
//	Expr  *ExpressionStm
//	In    bool
//	Exprs []*ExpressionStm
//}
//
//type ExpressionInSubQueryStm struct {
//	Expr     *ExpressionStm
//	In       bool
//	SubQuery SubQueryStm
//}
//
//type ExpressionLikeVariableStm struct {
//	Expr     *ExpressionStm
//	Like     bool
//	Variable *ExpressionStm
//}

type LiteralExpressionStm ColumnValue
type IdentifierExpression []byte

// FuncName(params...)
type FunctionCallExpressionStm struct {
	FuncName string
	Params   []*ExpressionStm
}

type SubExpressionTerm ExpressionTerm

type ExistsSubQueryExpressionStm struct {
	Exists   bool
	SubQuery SubQueryStm
}

type SubQueryStm *SelectStm

// Update statement is like:
// * update table_reference set assignments... [WhereStm] [OrderByStm] [LimitStm]
// * update table_reference... set assignments... [WhereStm]
type UpdateStm struct {
	TableRefs   []TableReferenceStm
	Assignments []AssignmentStm
	Where       WhereStm
	OrderBy     *OrderByStm
	Limit       *LimitStm
}

// A table reference statement is like:
// * {tb_name [as alias] | joined_table | table_subquery as alias} | (tableRef)
// * table_sub_query := (selectStm) as alias
type TableReferenceStm struct {
	Tp TableReferenceType
	// Can be TableReferenceTblStm or JoinedTableStm or TableSubQueryStm
	TableReference interface{}
}

type TableReferenceType byte

const (
	TableReferencePureTableNameTp TableReferenceType = iota
	TableReferenceJoinedTableTp
	TableReferenceTableSubQueryTp
	TableReferenceSubTableReferenceStmTP
)

type TableReferenceTblStm struct {
	TableName string
	Alias     string
}

// where joined_table is like:
// * table_reference { {left|right} [outer] join table_reference join_specification | inner join table_reference [join_specification]}
type JoinedTableStm struct {
	// Can be a sub tableReferenceStm or a tb_name [as alias]
	TableReference       interface{}
	JoinTp               JoinType
	JoinedTableReference interface{}
	JoinSpec             JoinSpecification
}

type JoinType byte

const (
	LeftOuterJoin JoinType = iota
	RightOuterJoin
	InnerJoin
)

// join_specification is like:
// on where_condition | using (col,...)
type JoinSpecification struct {
	Tp        JoinSpecificationTp
	Condition interface{}
}

type JoinSpecificationTp byte

const (
	JoinSpecificationON JoinSpecificationTp = iota
	JoinSpecificationUsing
)

type TableSubQueryStm struct {
	Select *SelectStm
	Alias  string
}

// ColName = expression
type AssignmentStm struct {
	ColName string
	Value   *ExpressionStm
}

type WhereStm *ExpressionStm

// order by expressions [asc|desc],...
// pure column can be seen as a kind of expression as well.
type OrderByStm struct {
	Expressions []*ExpressionStm
	Asc         []bool
}

// Limit statement is like limit {[offset,] row_counter | row_counter OFFSET offset}
type LimitStm struct {
	Count  int
	Offset int
}

// Delete statement is like:
// * delete from tb_name [whereStm] [OrderByStm] [LimitStm]
// * delete tb1,... from table_references [WhereStm]
type SingleDeleteStm struct {
	TableName string
	Where     WhereStm
	OrderBy   *OrderByStm
	Limit     *LimitStm
}

// * delete tb1,... from table_references [WhereStm]
type MultiDeleteStm struct {
	TableNames      []string
	TableReferences []TableReferenceStm
	Where           WhereStm
}

// Select statement is like:
// * select [all | distinct | distinctrow] select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm]
// [OrderByStm] [LimitStm] [for update | lock in share mode]
type SelectStm struct {
	Tp                SelectTp
	SelectExpressions *SelectExpressionStm
	TableReferences   []TableReferenceStm // If there are more tables, query are using join.
	Where             WhereStm
	OrderBy           *OrderByStm
	Groupby           *GroupByStm
	Having            HavingStm
	LimitStm          *LimitStm
	LockTp            SelectLockTp
}

type SelectExpressionStm struct {
	Tp   SelectExpressionTp
	Expr interface{}
}

type SelectExpressionTp byte

const (
	ExprSelectExpressionTp SelectExpressionTp = iota
	StarSelectExpressionTp
)

// group by {col_name | expressions [asc|desc]}...
// pure column can be seen as a kind of expression as well.
type GroupByStm OrderByStm

// Having WhereStm
type HavingStm WhereStm

type SelectTp byte

const (
	SelectAllTp SelectTp = iota
	SelectDistinctTp
	SelectDistinctRowTp
)

type SelectLockTp byte

const (
	ForUpdateLockTp SelectLockTp = iota
	LockInShareModeTp
	NoneLockTp
)
