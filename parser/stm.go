package parser

import "encoding/json"

type Stm interface {
	// Execute() error
}

// create database statement:
// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];
type CreateDatabaseStm struct {
	DatabaseName string
	IfNotExist   bool
	Charset      CharacterSetTP
	Collate      CollateTP
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
	Indexes     []*IndexDefStm
	Constraints []*ConstraintDefStm
	Engine      string
	Charset     CharacterSetTP
	Collate     CollateTP
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
	Tp     TokenType
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

// Rename statement can be rename table statement.
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
	Charset   CharacterSetTP
	Collate   CollateTP
}

type AlterTableAlterEngineStm struct {
	TableName string
	Engine    string
}

// Alter database statement can be:
// * alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]
type AlterDatabaseStm struct {
	DatabaseName string
	Charset      CharacterSetTP
	Collate      CollateTP
}

// Second DML
// Insert statement is like:
// * insert into tb_name [( col_name,... )] values (expression,...)
type InsertIntoStm struct {
	TableName string
	Cols      []string
	Values    []*ExpressionStm
}

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
// Note: literal can be -5
type ExpressionStm struct {
	LeftExpr  interface{}   `json:"left"` // can be ExpressionTerm or ExpressionStm
	Op        *ExpressionOp `json:"op"`
	RightExpr interface{}   `json:"right"`
}

type ExpressionTerm struct {
	UnaryOp      UnaryOpTp        `json:"unary"`
	Tp           ExpressionTermTP `json:"tp"`
	RealExprTerm interface{}      `json:"real_expr"` // can be LiteralExpressionStm, IdentifierExpression, FunctionCallExpressionStm, SubExpressionTerm
}

type UnaryOpTp byte

const (
	NoneUnaryOpTp UnaryOpTp = iota
	NegativeUnaryOpTp
)

// Todo.
type OrderedExpressionStm struct {
	Expression *ExpressionStm
	Asc        bool
}

type ExpressionTermTP byte

const (
	LiteralExpressionTermTP ExpressionTermTP = iota // raw value like string, char, float.
	SubExpressionTermTP
	IdentifierExpressionTermTP // should be column
	FuncCallExpressionTermTP
	AllExpressionTermTP // for count(*)
)

type ExpressionOp struct {
	Tp       TokenType
	Priority int
	Name     string
}

var (
	OperationAdd        = &ExpressionOp{Tp: ADD, Priority: 2, Name: "+"}
	OperationMinus      = &ExpressionOp{Tp: MINUS, Priority: 2, Name: "-"}
	OperationMul        = &ExpressionOp{Tp: MUL, Priority: 3, Name: "*"}
	OperationDivide     = &ExpressionOp{Tp: DIVIDE, Priority: 3, Name: "/"}
	OperationMod        = &ExpressionOp{Tp: MOD, Priority: 3, Name: "%"}
	OperationEqual      = &ExpressionOp{Tp: EQUAL, Priority: 1, Name: "="}
	OperationIs         = &ExpressionOp{Tp: IS, Priority: 1, Name: "is"}
	OperationNotEqual   = &ExpressionOp{Tp: NOTEQUAL, Priority: 1, Name: "!="}
	OperationGreat      = &ExpressionOp{Tp: GREAT, Priority: 1, Name: ">"}
	OperationGreatEqual = &ExpressionOp{Tp: GREATEQUAL, Priority: 1, Name: ">="}
	OperationLess       = &ExpressionOp{Tp: LESS, Priority: 1, Name: "<"}
	OperationLessEqual  = &ExpressionOp{Tp: LESSEQUAL, Priority: 1, Name: "<="}
	OperationAnd        = &ExpressionOp{Tp: AND, Priority: 0, Name: "and"}
	OperationOr         = &ExpressionOp{Tp: OR, Priority: 0, Name: "or"}
	// OperationISNot      ExpressionOp = ExpressionOp{Tp: lexer.OR + 1, Priority: 1}
	// OperationDot ExpressionOp = ExpressionOp{Tp: lexer.DOT, Priority: 2}
)

//type ExpressionInExpressionsStm struct {
//	Expr  *ExpressionStm
//	In    bool
//	Values []*ExpressionStm
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

func (stm LiteralExpressionStm) MarshalJSON() ([]byte, error) {
	v := string(stm)
	return json.Marshal(v)
}

type IdentifierExpression []byte

func (ident IdentifierExpression) MarshalJSON() ([]byte, error) {
	id := string(ident)
	return json.Marshal(id)
}

// Name(params...)
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
	TableRefs   TableReferenceStm
	Assignments []*AssignmentStm
	Where       WhereStm
	OrderBy     *OrderByStm
	Limit       *LimitStm
}

type MultiUpdateStm struct {
	TableRefs   []TableReferenceStm
	Assignments []*AssignmentStm
	Where       WhereStm
}

// A table reference statement is like:
// table_factor | joined_table
// where table_factor can be:
// * {tb_name [as alias] | (table_subquery) as alias} | (tableRef)
// and joined_table is like:
// * table_factor { {left|right} [outer] join table_reference join_specification | inner join table_factor [join_specification] } *
// join_specification is like:
// on where_condition | using (col...)

// Diff with mysql
// * index_hint are not supported.
// * cross join, straight join and natural join keywords are not supported.
type TableReferenceStm struct {
	Tp TableReferenceType
	// Can be JoinedTableStm or TableFactorStm
	TableReference interface{}
}

type TableReferenceType byte

const (
	TableReferenceTableFactorTp TableReferenceType = iota
	TableReferenceJoinTableTp
)

type TableReferenceTableFactorType byte

const (
	TableReferencePureTableNameTp TableReferenceTableFactorType = iota
	TableReferenceTableSubQueryTp
	TableReferenceSubTableReferenceStmTP
)

type TableReferenceTableFactorStm struct {
	Tp                   TableReferenceTableFactorType
	TableFactorReference interface{} // Can be TableSubQueryStm or TableReferencePureTableRefStm or TableReferenceStm (for subTable)
}

type TableReferencePureTableRefStm struct {
	TableName string
	Alias     string
}

// where joined_table is like:
// * table_factor { {left|right} [outer] join table_reference join_specification | inner join table_factor [join_specification] } *
// join_specification is like:
// on where_condition | using (col...)

type JoinedTableStm struct {
	TableFactor TableReferenceTableFactorStm
	JoinFactors []JoinFactor
}

type JoinFactor struct {
	JoinTp               JoinType
	JoinedTableReference TableReferenceStm
	JoinSpec             *JoinSpecification
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
	Expressions []*OrderedExpressionStm
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
	TableRef TableReferenceStm
	Where    WhereStm
	OrderBy  *OrderByStm
	Limit    *LimitStm
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

// selectExpr can use alias.
type SelectExpressionStm struct {
	Tp   SelectExpressionTp
	Expr interface{} // can be lexer.STAR or []*SelectExpr
}

type SelectExpressionTp byte

const (
	ExprSelectExpressionTp SelectExpressionTp = iota
	StarSelectExpressionTp
)

type SelectExpr struct {
	Expr  *ExpressionStm
	Alias string
}

// group by {expressions}...
type GroupByStm []*ExpressionStm

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
	NoneLockTp SelectLockTp = iota
	ForUpdateLockTp
	LockInShareModeTp
)

// use dataBaseName;
type UseDatabaseStm struct {
	DatabaseName string
}

type ShowStmTp byte

const (
	ShowTableTP ShowStmTp = iota
	ShowDatabaseTP
	ShowCreateTableTP
)

// show tables, show databases, show create table table_name
type ShowStm struct {
	TP    ShowStmTp
	Table string
}

type TransStm string

const (
	BeginStm    TransStm = "begin"
	RollbackStm          = "rollback"
	CommitStm            = "commit"
)

type CharacterSetTP string

const (
	UTF8TP                CharacterSetTP = "utf8"
	UTF16TP                              = "utf16"
	UTF32TP                              = "utf32"
	DEFAULTCHARACTERSETTP                = "default"
)

var characterSetMap = map[TokenType]CharacterSetTP{
	UTF8:    UTF8TP,
	UTF16:   UTF16TP,
	UTF32:   UTF32TP,
	DEFAULT: DEFAULTCHARACTERSETTP,
}

type CollateTP string

const (
	UTF8GENERALCITP  CollateTP = "UTF8GENERALCI"
	UTF16GENERALCITP           = "UTF16GENERALCI"
	UTF32GENERALCITP           = "UTF32GENERALCI"
	DEFAULTCOLLATETP           = "Default"
)

var collateMap = map[TokenType]CollateTP{
	UTF8GENERALCI:  UTF8GENERALCITP,
	UTF16GENERALCI: UTF16GENERALCITP,
	UTF32GENERALCI: UTF32GENERALCITP,
	DEFAULT:        DEFAULTCOLLATETP,
}
