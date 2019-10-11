package ast

import (
	"simpleDb/lexer"
)

type Stm interface {
	Stms() []Stm
}

var EmptySqlStms = SqlStms{}

type SqlStms struct {
	Stms []Stm
}

type CreateTableStm struct {
	TableName  string
	IfNotExist bool
	Cols       []*ColumnDefStm
}

func NewCreateTableStm(tableName string, ifNotExist bool) *CreateTableStm {
	return &CreateTableStm{TableName: tableName, IfNotExist: ifNotExist}
}

func (stm *CreateTableStm) Stms() []Stm {
	return nil
}

func (stm *CreateTableStm) AppendCol(col *ColumnDefStm) {
	stm.Cols = append(stm.Cols, col)
}

func (stm *CreateTableStm) IsTableNameEmpty() bool {
	return stm.TableName == ""
}

type CreateDatabaseStm struct {
	DatabaseName string
	IfNotExist   bool
}

func (stm *CreateDatabaseStm) Stms() []Stm {
	return nil
}

func NewCreateDatabaseStm(databaseName string, ifNotExist bool) *CreateDatabaseStm {
	return &CreateDatabaseStm{DatabaseName: databaseName, IfNotExist: ifNotExist}
}

type ColumnDefStm struct {
	OldColName string
	ColName    string
	ColValue   ColumnValue
	PrimaryKey bool
	ColumnType ColumnType
}

func NewColumnStm(columnName string) *ColumnDefStm {
	return &ColumnDefStm{ColName: columnName}
}

func (stm *ColumnDefStm) Stms() []Stm {
	return nil
}

type ColumnRefStm string

func (stm *ColumnRefStm) Stms() []Stm {
	return nil
}

type ColumnType struct {
	Tp  lexer.TokenType
	Min int
	Max int
}

func NewColumnType(tp lexer.TokenType, min, max int) ColumnType {
	return ColumnType{Tp: tp, Min: min, Max: max}
}

type DropDatabaseStm struct {
	DatabaseName string
	IfExist      bool
}

func NewDropDatabaseStm(databaseName string, ifExist bool) *DropDatabaseStm {
	return &DropDatabaseStm{databaseName, ifExist}
}

func (stm *DropDatabaseStm) Stms() []Stm {
	return nil
}

type DropTableStm struct {
	IfExist    bool
	TableNames []string
}

func NewDropTableStm(ifExist bool, tableName ...string) *DropTableStm {
	return &DropTableStm{ifExist, tableName}
}

func (stm *DropTableStm) Stms() []Stm {
	return nil
}

type DeleteStm struct {
	TableName  string
	WhereStm   *WhereStm
	OrderByStm *OrderByStm
	LimitStm   *LimitStm
}

func (stm *DeleteStm) Stms() []Stm {
	return nil
}

func NewDeleteStm(tableName string, whereStm *WhereStm, orderByStm *OrderByStm, limitStm *LimitStm) *DeleteStm {
	return &DeleteStm{tableName, whereStm, orderByStm, limitStm}
}

type InsertIntoStm struct {
	TableName        string
	Cols             []string
	ValueExpressions []Stm
}

func (stm *InsertIntoStm) Stms() []Stm {
	return nil
}

type ColumnValue struct {
	ValueType lexer.TokenType
	Value     interface{}
}

func (stm *ColumnValue) Stms() []Stm {
	return nil
}

var EmptyColumnValue = ColumnValue{}

func NewColumnValue(valueType lexer.TokenType, value interface{}) ColumnValue {
	return ColumnValue{valueType, value}
}

type WhereStm struct {
	ExpressionStms Stm
}

func (stm *WhereStm) Stms() []Stm {
	return nil
}

type ExpressionStm struct {
	Params []Stm
}

type OperationStm lexer.TokenType

func (stm *OperationStm) Stms() []Stm {
	return nil
}

type FunctionCallStm struct {
	FuncName string
	Params   []Stm
}

func (stm *FunctionCallStm) Stms() []Stm {
	return nil
}

func (stm *ExpressionStm) Stms() []Stm {
	return stm.Params
}

func (stm *ExpressionStm) Append(s Stm) {
	stm.Params = append(stm.Params, s)
}

type OrderByStm struct {
	Cols []string
}

func (stm *OrderByStm) Stms() []Stm {
	return nil
}

type LimitStm struct {
	Count int
}

func (stm LimitStm) Stms() []Stm {
	return nil
}

type TruncateStm struct {
	TableName string
}

func (stm *TruncateStm) Stms() []Stm {
	return nil
}

type RenameStm struct {
	Tp            lexer.TokenType
	OrigNames     string
	ModifiedNames string
}

func (stm *RenameStm) Stms() []Stm {
	return nil
}

type SelectStm struct {
	Expressions []Stm
	TableName   string
	WhereStm    Stm
	OrderByStm  Stm
	LimitStm    Stm
}

func (stm *SelectStm) Stms() []Stm {
	return nil
}

type UpdateStm struct {
	TableName   string
	Expressions []Stm
	WhereStm    *WhereStm
}

func (stm *UpdateStm) Stms() []Stm {
	return nil
}

type AlterStm struct {
	TableName string
	Tp        lexer.TokenType
	ColDef    *ColumnDefStm
}

func (stm *AlterStm) Stms() []Stm {
	return nil
}
