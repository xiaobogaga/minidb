package plan

import (
	"bytes"
	"errors"
	"fmt"
	"simpleDb/storage"
	"strings"
)

type LogicExpr interface {
	toField(input LogicPlan) storage.Field
	String() string
	TypeCheck(input LogicPlan) error
	Evaluate(input storage.RecordBatch) storage.ColumnVector
}

type LogicExprTP byte

const (
	IdentifierLogicExprTP LogicExprTP = iota
	LiteralLogicExprTP
	NegativeLogicExprTP
	AddLogicExprTP
	MinusLogicExprTP
	MulLogicExprTP
	DivideLogicExprTP
	ModLogicExprTP
	EqualLogicExprTP
	IsLogicExprTP
	NotEqualLogicExprTP
	GreatLogicExprTP
	GreatEqualLogicExprTP
	LessLogicExprTP
	LessEqualLogicExprTP
	AndLogicExprTP
	OrLogicExprTP
	// DotLogicExprTP
	OrderedLogicExprTP
	FuncCallLogicExprTP
)

// can be a.b.c or a.b or a
type IdentifierLogicExpr struct {
	Ident        []byte
	IdentifierTp IdentifierTP
	Schema       string
	Table        string
	Column       string
}

type IdentifierTP byte

const (
	SchemaNameTP IdentifierTP = iota
	TableNameTP
	ColumnNameTP
)

func (ident IdentifierLogicExpr) toField(input LogicPlan) storage.Field {
	schema := input.Schema()
	return schema.GetField(ident.Column)
}

func (ident IdentifierLogicExpr) String() string {
	return string(ident.Ident)
}

func (ident IdentifierLogicExpr) TypeCheck(input LogicPlan) error {
	word := string(ident.Ident)
	splits := strings.Split(word, ".")
	switch len(splits) {
	case 3:
		ident.Schema = splits[0]
		ident.Table = splits[1]
		ident.Column = splits[2]
	case 2:
		ident.Table = splits[0]
		ident.Column = splits[1]
	case 1:
		ident.Column = splits[0]
	}
	schema := input.Schema()
	// Now we check whether we can find such column.
	if !schema.HasColumn(ident.Schema, ident.Table, ident.Column) {
		return errors.New(fmt.Sprintf("column %s cannot find", ident.Column))
	}
	if schema.HasAmbiguousColumn(ident.Schema, ident.Table, ident.Column) {
		return errors.New(fmt.Sprintf("column %s is ambiguous", ident.Column))
	}
	return nil
}

func (ident IdentifierLogicExpr) Evaluate(input storage.RecordBatch) storage.ColumnVector {
	return input.Records[ident.Column]
}

type LiteralLogicExpr struct {
	TP   storage.FieldTP
	Data []byte
}

func (literal LiteralLogicExpr) toField(input LogicPlan) storage.Field {
	return inferenceType(literal.Data)
}

func (literal LiteralLogicExpr) TypeCheck(input LogicPlan) error {
	// No error for literal
	return nil
}

func (literal LiteralLogicExpr) String() string {
	return string(literal.Data)
}

func (literal LiteralLogicExpr) Evaluate(input storage.RecordBatch) storage.ColumnVector {
	ret := storage.ColumnVector{
		Field: inferenceType(literal.Data),
		Values: ,
	}
	return ret
}

type NegativeLogicExpr struct {
	Input LogicExpr
	Name  string
	Alias string
}

func (negative NegativeLogicExpr) toField(input LogicPlan) storage.Field {
	// Todo
	return negative.Input.toField(input)
}

func (negative NegativeLogicExpr) TypeCheck(input LogicPlan) error {
	err := negative.Input.TypeCheck(input)
	if err != nil {
		return err
	}
	field := negative.Input.toField(input)
	if !storage.IsFieldNumerialType(field) {
		return errors.New("- cannot applied to non numerical type")
	}
	return nil
}

func (negative NegativeLogicExpr) String() string {
	return fmt.Sprintf("-%s", negative.Input)
}

func (negative NegativeLogicExpr) Evaluate(input storage.RecordBatch) storage.ColumnVector {

}

// Math expr
type AddLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (add AddLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := add.Left.toField(input)
	rightInputField := add.Right.toField(input)
	tp := inferenceTypeAfterOp(leftInputField, rightInputField, AddLogicExprTP)
	f := storage.Field{Name: add.Name, TP: tp}
	return f
}

func (add AddLogicExpr) String() string {
	return fmt.Sprintf("Add(%s, %s)", add.Left, add.Right)
}

func (add AddLogicExpr) TypeCheck(input LogicPlan) error {
	err := add.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = add.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := add.Left.toField(input)
	field2 := add.Right.toField(input)
	if !storage.IsFieldNumerialType(field1) || !storage.IsFieldNumerialType(field2) {
		return errors.New("+ cannot applied to non numerical type")
	}
	return nil
}

func (add AddLogicExpr) Evaluate(input storage.RecordBatch) storage.ColumnVector {

}

type MinusLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (minus MinusLogicExpr) toField(input LogicPlan) storage.Field {}
func (minus MinusLogicExpr) String() string {
	return fmt.Sprintf("Minus(%s, %s)", minus.Left, minus.Right)
}

func (minus MinusLogicExpr) TypeCheck(input LogicPlan) error {}

func (minus MinusLogicExpr) Evaluate() {}

type MulLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (mul MulLogicExpr) toField(input LogicPlan) storage.Field {}
func (mul MulLogicExpr) String() string {
	return fmt.Sprintf("Mul(%s, %s)", mul.Left, mul.Right)
}

func (mul MulLogicExpr) TypeCheck(input LogicPlan) error {}
func (mul MulLogicExpr) Evaluate()                       {}

type DivideLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (divide DivideLogicExpr) toField(input LogicPlan) storage.Field {}
func (divide DivideLogicExpr) String() string {
	return fmt.Sprintf("Divide(%s, %s)", divide.Left, divide.Right)
}
func (divide DivideLogicExpr) TypeCheck(input LogicPlan) error {}
func (divide DivideLogicExpr) Evaluate()                       {}

type ModLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (mod ModLogicExpr) toField(input LogicPlan) storage.Field {}
func (mod ModLogicExpr) String() string {
	return fmt.Sprintf("Mod(%s, %s)", mod.Left, mod.Right)
}
func (mod ModLogicExpr) TypeCheck(input LogicPlan) error {}
func (mod ModLogicExpr) Evaluate()                       {}

type EqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (equal EqualLogicExpr) toField(input LogicPlan) storage.Field {}
func (equal EqualLogicExpr) String() string {
	return fmt.Sprintf("Equal(%s, %s)", equal.Left, equal.Right)
}

func (equal EqualLogicExpr) TypeCheck(input LogicPlan) error {}
func (equal EqualLogicExpr) Evaluate()                       {}

type IsLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (is IsLogicExpr) toField(input LogicPlan) storage.Field {}
func (is IsLogicExpr) String() string {
	return fmt.Sprintf("IS(%s, %s)", is.Left, is.Right)
}
func (is IsLogicExpr) TypeCheck(input LogicPlan) error {}
func (is IsLogicExpr) Evaluate()                       {}

type NotEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (notEqual NotEqualLogicExpr) toField(input LogicPlan) storage.Field {}
func (notEqual NotEqualLogicExpr) String() string {
	return fmt.Sprintf("NotEqual(%s, %s)", notEqual.Left, notEqual.Right)
}
func (notEqual NotEqualLogicExpr) TypeCheck(input LogicPlan) error {}
func (notEqual NotEqualLogicExpr) Evaluate()                       {}

type GreatLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (great GreatLogicExpr) toField(input LogicPlan) storage.Field {}
func (great GreatLogicExpr) String() string {
	return fmt.Sprintf("Great(%s, %s)", great.Left, great.Right)
}
func (great GreatLogicExpr) TypeCheck(input LogicPlan) error {}
func (great GreatLogicExpr) Evaluate()                       {}

type GreatEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (greatEqual GreatEqualLogicExpr) toField(input LogicPlan) storage.Field {}
func (greatEqual GreatEqualLogicExpr) String() string {
	return fmt.Sprintf("GreatEqual(%s, %s)", greatEqual.Left, greatEqual.Right)
}
func (greatEqual GreatEqualLogicExpr) TypeCheck(input LogicPlan) error {}
func (greatEqual GreatEqualLogicExpr) Evaluate()                       {}

type LessLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (less LessLogicExpr) toField(input LogicPlan) storage.Field {}
func (less LessLogicExpr) String() string {
	return fmt.Sprintf("Less(%s, %s)", less.Left, less.Right)
}
func (less LessLogicExpr) TypeCheck(input LogicPlan) error {}
func (less LessLogicExpr) Evaluate()                       {}

type LessEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (lessEqual LessEqualLogicExpr) toField(input LogicPlan) storage.Field {}
func (lessEqual LessEqualLogicExpr) String() string {
	return fmt.Sprintf("LessEqual(%s, %s)", lessEqual.Left, lessEqual.Right)
}
func (lessEqual LessEqualLogicExpr) TypeCheck(input LogicPlan) error {}
func (lessEqual LessEqualLogicExpr) Evaluate()                       {}

type AndLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (and AndLogicExpr) toField(input LogicPlan) storage.Field {}
func (and AndLogicExpr) String() string {
	return fmt.Sprintf("AND(%s, %s)", and.Left, and.Right)
}
func (and AndLogicExpr) TypeCheck(input LogicPlan) error {}
func (and AndLogicExpr) Evaluate()                       {}

type OrLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
}

func (or OrLogicExpr) toField(input LogicPlan) storage.Field {}
func (or OrLogicExpr) String() string {
	return fmt.Sprintf("OR(%s, %s)", or.Left, or.Right)
}
func (or OrLogicExpr) TypeCheck(input LogicPlan) error {}
func (or OrLogicExpr) Evaluate()                       {}

type AggrExpr struct{}

type OrderedLogicExpr struct {
	expr []LogicExpr
	asc  []bool
}

func (orderBy OrderedLogicExpr) toField(input LogicPlan) storage.Field {}
func (orderBy OrderedLogicExpr) String() string                        {}
func (orderBy OrderedLogicExpr) TypeCheck(input LogicPlan) error       {}
func (orderBy OrderedLogicExpr) Evaluate()                             {}

type FuncCallLogicExpr struct {
	FuncName string
	Params   []LogicExpr
}

func (call FuncCallLogicExpr) toField(input LogicPlan) storage.Field {

}

func (call FuncCallLogicExpr) String() string {
	bf := bytes.Buffer{}
	bf.WriteString(call.FuncName + "(")
	for i, param := range call.Params {
		bf.WriteString(param.String())
		if i != len(call.Params)-1 {
			bf.WriteString(",")
		}
	}
	bf.WriteString(")")
	return bf.String()
}

func (call FuncCallLogicExpr) TypeCheck(input LogicPlan) error {}
func (call FuncCallLogicExpr) Evaluate()                       {}

func inferenceType(data []byte) storage.Field {

}

func inferenceTypeAfterOp(leftField, rightField storage.Field, op LogicExprTP) storage.FieldTP {

}
