package plan

import (
	"bytes"
	"errors"
	"fmt"
	"simpleDb/storage"
	"strings"
)

type LogicExpr interface {
	toField() storage.Field
	String() string
	TypeCheck() error
	AggrTypeCheck(groupByExpr []LogicExpr) error
	Evaluate(input *storage.RecordBatch) storage.ColumnVector
	EvaluateRow(row int, input *storage.RecordBatch) []byte
	Accumulate(row int, input *storage.RecordBatch) // Accumulate the value.
	AccumulateValue() []byte
	Clone(cloneAccumulator bool) LogicExpr
	HasGroupFunc() bool
	Compute() ([]byte, error) // For insert, update, delete.
}

// can be a.b.c or a.b or a
type IdentifierLogicExpr struct {
	Ident       []byte
	Schema      string
	Table       string
	Column      string
	accumulator []byte // put accumulator here is not a good idea. It's better to separate.
	input       LogicPlan
}

func (ident IdentifierLogicExpr) toField() storage.Field {
	// The column must be unique in the input schema.
	schema := ident.input.Schema()
	return schema.GetField(ident.Column)
}

func (ident IdentifierLogicExpr) String() string {
	return string(ident.Ident)
}

func (ident IdentifierLogicExpr) TypeCheck() error {
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
	schema := ident.input.Schema()
	// Now we check whether we can find such column.
	if !schema.HasColumn(ident.Schema, ident.Table, ident.Column) {
		return errors.New(fmt.Sprintf("column %s cannot find", ident.Column))
	}
	if schema.HasAmbiguousColumn(ident.Schema, ident.Table, ident.Column) {
		return errors.New(fmt.Sprintf("column %s is ambiguous", ident.Column))
	}
	return nil
}

func (ident IdentifierLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	return input.GetColumnValue(ident.Column)
}

func (ident IdentifierLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	return input.GetColumnValue(ident.Column).RawValue(row)
}

func (ident IdentifierLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	// When encounter groupBy clause, the expression used in select ..., and in orderBy
	// in having clause, must match the groupByExpr.
	// How we do aggregation type check?
	// Let's say we have a table called `mytest`.
	// ++++++++
	// |    id|
	// |  name|
	// |   age|
	// ++++++++
	// for query, select id, name from mytest group by id.
	// this query is not legal because name is not in the group by clause.
	// However, this query works:
	// select id * age + 1 from mytest group by id, age;
	for _, expr := range groupByExpr {
		if ident.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", ident))
}

func (ident IdentifierLogicExpr) Clone(cloneAccumulate bool) LogicExpr {
	ret := IdentifierLogicExpr{
		Ident:  ident.Ident,
		Schema: ident.Schema,
		Table:  ident.Table,
		Column: ident.Column,
	}
	if cloneAccumulate {
		ret.accumulator = ident.accumulator
	}
	return ret
}

func (ident IdentifierLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	col := input.GetColumnValue(ident.Column)
	ident.accumulator = col.Values[row]
}

func (ident IdentifierLogicExpr) AccumulateValue() []byte {
	return ident.accumulator
}

func (ident IdentifierLogicExpr) HasGroupFunc() bool { return false }

func (ident IdentifierLogicExpr) Compute() ([]byte, error) {
	return nil, errors.New("unsupported action")
}

type LiteralLogicExpr struct {
	TP storage.FieldTP
	// Data is a bytes array, which might be a "xxx", or 'xxx' or true, false, or numerical value such as .10100, 01001, 909008
	// when we inference the type of data, it can be a numerical, bool, string, datetime, blob.
	// Numerical, bool, string can be easily fixed.
	// datetime must be a string start with '2020-10-12 10-13-14', or '2020-10-12 10-13-14' etc.
	// In mysql, some literal value can be transformed to another type, for example, true can be trans to integer 1.
	// But we will use a strict type. True cannot be transformed to 1.
	// For Blob, they must be a string 'xxx' or "xxx".
	Data []byte
}

func (literal LiteralLogicExpr) toField() storage.Field {
	f := storage.Field{Name: string(literal.Data)}
	f.TP = storage.InferenceType(literal.Data)
	return f
}

func (literal LiteralLogicExpr) TypeCheck() error {
	return nil
}

func (literal LiteralLogicExpr) String() string {
	return string(literal.Data)
}

func (literal LiteralLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	ret := storage.ColumnVector{
		Field: storage.Field{Name: string(literal.Data), TP: storage.InferenceType(literal.Data)},
	}
	for i := 0; i < input.RowCount(); i++ {
		ret.Values = append(ret.Values, literal.Data)
	}
	return ret
}

func (literal LiteralLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	return literal.Data
}

func (literal LiteralLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	return nil
}

func (literal LiteralLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	return
}

func (literal LiteralLogicExpr) AccumulateValue() []byte {
	return literal.Data
}

func (literal LiteralLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return literal
}

func (literal LiteralLogicExpr) HasGroupFunc() bool { return false }

func (literal LiteralLogicExpr) Compute() ([]byte, error) {
	return literal.Data, nil
}

type NegativeLogicExpr struct {
	Expr  LogicExpr
	Name  string
	Alias string
}

func (negative NegativeLogicExpr) toField() storage.Field {
	field := negative.Expr.toField()
	ret := storage.Field{
		TP:         field.TP,
		Name:       negative.String(),
		TableName:  field.TableName,
		SchemaName: field.SchemaName,
	}
	return ret
}

func (negative NegativeLogicExpr) TypeCheck() error {
	err := negative.Expr.TypeCheck()
	if err != nil {
		return err
	}
	field := negative.Expr.toField()
	return field.CanOp(field, storage.NegativeOpType)
}

func (negative NegativeLogicExpr) String() string {
	return fmt.Sprintf("-%s", negative.Expr)
}

func (negative NegativeLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	columnVector := negative.Expr.Evaluate(input)
	return columnVector.Negative()
}

func (negative NegativeLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	data := negative.Expr.EvaluateRow(row, input)
	return storage.Negative(negative.toField().TP, data)
}

func (negative NegativeLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	// It works for query:
	// select -id from mytest group by id
	// select -id from mytest group by -id
	err := negative.Expr.AggrTypeCheck(groupByExpr)
	if err == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if negative.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", negative))
}

func (negative NegativeLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	negative.Expr.Accumulate(row, input)
}

func (negative NegativeLogicExpr) AccumulateValue() []byte {
	val := negative.Expr.AccumulateValue()
	return storage.Negative(negative.toField().TP, val)
}

func (negative NegativeLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return NegativeLogicExpr{
		Expr:  negative.Expr.Clone(cloneAccumulator),
		Name:  negative.Name,
		Alias: negative.Alias,
	}
}

func (negative NegativeLogicExpr) HasGroupFunc() bool {
	return negative.Expr.HasGroupFunc()
}

func (negative NegativeLogicExpr) Compute() ([]byte, error) {
	val, err := negative.Expr.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Negative(negative.toField().TP, val), nil
}

// Math expr
type AddLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (add AddLogicExpr) toField() storage.Field {
	leftInputField := add.Left.toField()
	rightInputField := add.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.AddOpType)
	f := storage.Field{Name: add.String(), TP: tp}
	return f
}

func (add AddLogicExpr) String() string {
	return fmt.Sprintf("Add(%s, %s)", add.Left, add.Right)
}

func (add AddLogicExpr) TypeCheck() error {
	err := add.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = add.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := add.Left.toField()
	field2 := add.Right.toField()
	return field1.CanOp(field2, storage.AddOpType)
}

func (add AddLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := add.Left.Evaluate(input)
	rightColumnVector := add.Right.Evaluate(input)
	return leftColumnVector.Add(rightColumnVector, add.String())
}

func (add AddLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := add.Left.EvaluateRow(row, input)
	val2 := add.Right.EvaluateRow(row, input)
	return storage.Add(val1, add.Left.toField().TP, val2, add.Right.toField().TP)
}

func (add AddLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	// It works for query:
	// select id + age from mytest group by id, age
	// select id + 5 from mytest group by id
	// select id + 5 from mytest group by id + 5
	// It doesn't works for:
	// select id + 5 from mytest group by id + 6
	// select id + 2 + 3 from mytest group by id + 2 + 2 + 1
	if add.Left.AggrTypeCheck(groupByExpr) == nil && add.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if add.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", add))
}

func (add AddLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	add.Left.Accumulate(row, input)
	add.Right.Accumulate(row, input)
}

func (add AddLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := add.Left.AccumulateValue()
	rightAccumulateValue := add.Right.AccumulateValue()
	return storage.Add(leftAccumulateValue, add.Left.toField().TP, rightAccumulateValue,
		add.Right.toField().TP)
}

func (add AddLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return AddLogicExpr{
		Left:  add.Left.Clone(cloneAccumulator),
		Right: add.Right.Clone(cloneAccumulator),
		Name:  add.Name,
		Alias: add.Alias,
	}
}

func (add AddLogicExpr) HasGroupFunc() bool {
	return add.Left.HasGroupFunc() || add.Right.HasGroupFunc()
}

func (add AddLogicExpr) Compute() ([]byte, error) {
	val1, err := add.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := add.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Add(val1, add.Left.toField().TP, val2, add.Right.toField().TP), nil
}

type MinusLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (minus MinusLogicExpr) toField() storage.Field {
	leftInputField := minus.Left.toField()
	rightInputField := minus.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.MinusOpType)
	f := storage.Field{Name: minus.String(), TP: tp}
	return f
}

func (minus MinusLogicExpr) String() string {
	return fmt.Sprintf("Minus(%s, %s)", minus.Left, minus.Right)
}

func (minus MinusLogicExpr) TypeCheck() error {
	err := minus.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = minus.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := minus.Left.toField()
	field2 := minus.Right.toField()
	return field1.CanOp(field2, storage.MinusOpType)
}

func (minus MinusLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if minus.Left.AggrTypeCheck(groupByExpr) == nil && minus.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if minus.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", minus))
}

func (minus MinusLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := minus.Left.Evaluate(input)
	rightColumnVector := minus.Right.Evaluate(input)
	return leftColumnVector.Minus(rightColumnVector, minus.String())
}

func (minus MinusLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := minus.Left.EvaluateRow(row, input)
	val2 := minus.Right.EvaluateRow(row, input)
	return storage.Minus(val1, minus.Left.toField().TP, val2, minus.Right.toField().TP)
}

func (minus MinusLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	minus.Left.Accumulate(row, input)
	minus.Right.Accumulate(row, input)
}

func (minus MinusLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := minus.Left.AccumulateValue()
	rightAccumulateValue := minus.Right.AccumulateValue()
	return storage.Minus(leftAccumulateValue, minus.Left.toField().TP, rightAccumulateValue,
		minus.Right.toField().TP)
}

func (minus MinusLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return MinusLogicExpr{
		Left:  minus.Left.Clone(cloneAccumulator),
		Right: minus.Right.Clone(cloneAccumulator),
		Name:  minus.Name,
		Alias: minus.Alias,
	}
}

func (minus MinusLogicExpr) HasGroupFunc() bool {
	return minus.Left.HasGroupFunc() || minus.Right.HasGroupFunc()
}

func (minus MinusLogicExpr) Compute() ([]byte, error) {
	val1, err := minus.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := minus.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Minus(val1, minus.Left.toField().TP, val2, minus.Right.toField().TP), nil
}

type MulLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (mul MulLogicExpr) toField() storage.Field {
	leftInputField := mul.Left.toField()
	rightInputField := mul.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.MulOpType)
	f := storage.Field{Name: mul.String(), TP: tp}
	return f
}

func (mul MulLogicExpr) String() string {
	return fmt.Sprintf("Mul(%s, %s)", mul.Left, mul.Right)
}

func (mul MulLogicExpr) TypeCheck() error {
	err := mul.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = mul.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := mul.Left.toField()
	field2 := mul.Right.toField()
	return field1.CanOp(field2, storage.MulOpType)
}

func (mul MulLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := mul.Left.Evaluate(input)
	rightColumnVector := mul.Right.Evaluate(input)
	return leftColumnVector.Mul(rightColumnVector, mul.String())
}

func (mul MulLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := mul.Left.EvaluateRow(row, input)
	val2 := mul.Right.EvaluateRow(row, input)
	return storage.Mul(val1, mul.Left.toField().TP, val2, mul.Right.toField().TP)
}

func (mul MulLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if mul.Left.AggrTypeCheck(groupByExpr) == nil && mul.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if mul.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", mul))
}

func (mul MulLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	mul.Left.Accumulate(row, input)
	mul.Right.Accumulate(row, input)
}

func (mul MulLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := mul.Left.AccumulateValue()
	rightAccumulateValue := mul.Right.AccumulateValue()
	return storage.Mul(leftAccumulateValue, mul.Left.toField().TP, rightAccumulateValue,
		mul.Right.toField().TP)
}

func (mul MulLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return MulLogicExpr{
		Left:  mul.Left.Clone(cloneAccumulator),
		Right: mul.Right.Clone(cloneAccumulator),
		Name:  mul.Name,
		Alias: mul.Alias,
	}
}

func (mul MulLogicExpr) HasGroupFunc() bool {
	return mul.Left.HasGroupFunc() || mul.Right.HasGroupFunc()
}

func (mul MulLogicExpr) Compute() ([]byte, error) {
	val1, err := mul.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := mul.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Mul(val1, mul.Left.toField().TP, val2, mul.Right.toField().TP), nil
}

type DivideLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (divide DivideLogicExpr) toField() storage.Field {
	leftInputField := divide.Left.toField()
	rightInputField := divide.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.DivideOpType)
	f := storage.Field{Name: divide.String(), TP: tp}
	return f
}

func (divide DivideLogicExpr) String() string {
	return fmt.Sprintf("Divide(%s, %s)", divide.Left, divide.Right)
}

func (divide DivideLogicExpr) TypeCheck() error {
	err := divide.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = divide.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := divide.Left.toField()
	field2 := divide.Right.toField()
	return field1.CanOp(field2, storage.DivideOpType)
}

func (divide DivideLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := divide.Left.Evaluate(input)
	rightColumnVector := divide.Right.Evaluate(input)
	return leftColumnVector.Divide(rightColumnVector, divide.String())
}

func (divide DivideLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := divide.Left.EvaluateRow(row, input)
	val2 := divide.Right.EvaluateRow(row, input)
	return storage.Divide(val1, divide.Left.toField().TP, val2, divide.Right.toField().TP)
}

func (divide DivideLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if divide.Left.AggrTypeCheck(groupByExpr) == nil && divide.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if divide.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", divide))
}

func (divide DivideLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	divide.Left.Accumulate(row, input)
	divide.Right.Accumulate(row, input)
}

func (divide DivideLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := divide.Left.AccumulateValue()
	rightAccumulateValue := divide.Right.AccumulateValue()
	return storage.Divide(leftAccumulateValue, divide.Left.toField().TP, rightAccumulateValue,
		divide.Right.toField().TP)
}

func (divide DivideLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return DivideLogicExpr{
		Left:  divide.Left.Clone(cloneAccumulator),
		Right: divide.Right.Clone(cloneAccumulator),
		Name:  divide.Name,
		Alias: divide.Alias,
	}
}

func (divide DivideLogicExpr) HasGroupFunc() bool {
	return divide.Left.HasGroupFunc() || divide.Right.HasGroupFunc()
}

func (divide DivideLogicExpr) Compute() ([]byte, error) {
	val1, err := divide.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := divide.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Divide(val1, divide.Left.toField().TP, val2, divide.Right.toField().TP), nil
}

type ModLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (mod ModLogicExpr) toField() storage.Field {
	leftInputField := mod.Left.toField()
	rightInputField := mod.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.ModOpType)
	f := storage.Field{Name: mod.String(), TP: tp}
	return f
}

func (mod ModLogicExpr) String() string {
	return fmt.Sprintf("Mod(%s, %s)", mod.Left, mod.Right)
}

func (mod ModLogicExpr) TypeCheck() error {
	err := mod.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = mod.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := mod.Left.toField()
	field2 := mod.Right.toField()
	return field1.CanOp(field2, storage.ModOpType)
}

func (mod ModLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := mod.Left.Evaluate(input)
	rightColumnVector := mod.Right.Evaluate(input)
	return leftColumnVector.Mod(rightColumnVector, mod.String())
}

func (mod ModLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := mod.Left.EvaluateRow(row, input)
	val2 := mod.Right.EvaluateRow(row, input)
	return storage.Mod(val1, mod.Left.toField().TP, val2, mod.Right.toField().TP)
}

func (mod ModLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if mod.Left.AggrTypeCheck(groupByExpr) == nil && mod.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if mod.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", mod))
}

func (mod ModLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	mod.Left.Accumulate(row, input)
	mod.Right.Accumulate(row, input)
}

func (mod ModLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := mod.Left.AccumulateValue()
	rightAccumulateValue := mod.Right.AccumulateValue()
	return storage.Mod(leftAccumulateValue, mod.Left.toField().TP, rightAccumulateValue,
		mod.Right.toField().TP)
}

func (mod ModLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return ModLogicExpr{
		Left:  mod.Left.Clone(cloneAccumulator),
		Right: mod.Right.Clone(cloneAccumulator),
		Name:  mod.Name,
	}
}

func (mod ModLogicExpr) HasGroupFunc() bool {
	return mod.Left.HasGroupFunc() || mod.Right.HasGroupFunc()
}

func (mod ModLogicExpr) Compute() ([]byte, error) {
	val1, err := mod.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := mod.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Mod(val1, mod.Left.toField().TP, val2, mod.Right.toField().TP), nil
}

type EqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (equal EqualLogicExpr) toField() storage.Field {
	leftInputField := equal.Left.toField()
	rightInputField := equal.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.EqualOpType)
	f := storage.Field{Name: equal.String(), TP: tp}
	return f
}

func (equal EqualLogicExpr) String() string {
	return fmt.Sprintf("Equal(%s, %s)", equal.Left, equal.Right)
}

func (equal EqualLogicExpr) TypeCheck() error {
	err := equal.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = equal.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := equal.Left.toField()
	field2 := equal.Right.toField()
	return field1.CanOp(field2, storage.EqualOpType)
}

func (equal EqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := equal.Left.Evaluate(input)
	rightColumnVector := equal.Right.Evaluate(input)
	return leftColumnVector.Equal(rightColumnVector, equal.String())
}

func (equal EqualLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := equal.Left.EvaluateRow(row, input)
	val2 := equal.Right.EvaluateRow(row, input)
	return storage.Equal(val1, equal.Left.toField().TP, val2, equal.Right.toField().TP)
}

func (equal EqualLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if equal.Left.AggrTypeCheck(groupByExpr) == nil && equal.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if equal.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", equal))
}

func (equal EqualLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	equal.Left.Accumulate(row, input)
	equal.Right.Accumulate(row, input)
}

func (equal EqualLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := equal.Left.AccumulateValue()
	rightAccumulateValue := equal.Right.AccumulateValue()
	return storage.Equal(leftAccumulateValue, equal.Left.toField().TP, rightAccumulateValue,
		equal.Right.toField().TP)
}

func (equal EqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return EqualLogicExpr{
		Left:  equal.Left.Clone(cloneAccumulator),
		Right: equal.Right.Clone(cloneAccumulator),
		Name:  equal.Name,
	}
}

func (equal EqualLogicExpr) HasGroupFunc() bool {
	return equal.Left.HasGroupFunc() || equal.Right.HasGroupFunc()
}

func (equal EqualLogicExpr) Compute() ([]byte, error) {
	val1, err := equal.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := equal.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Equal(val1, equal.Left.toField().TP, val2, equal.Right.toField().TP), nil
}

type IsLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (is IsLogicExpr) toField() storage.Field {
	leftInputField := is.Left.toField()
	rightInputField := is.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.IsOpType)
	f := storage.Field{Name: is.String(), TP: tp}
	return f
}

func (is IsLogicExpr) String() string {
	return fmt.Sprintf("IS(%s, %s)", is.Left, is.Right)
}
func (is IsLogicExpr) TypeCheck() error {
	err := is.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = is.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := is.Left.toField()
	field2 := is.Right.toField()
	return field1.CanOp(field2, storage.IsOpType)
}

func (is IsLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := is.Left.Evaluate(input)
	rightColumnVector := is.Right.Evaluate(input)
	return leftColumnVector.Is(rightColumnVector, is.String())
}

func (is IsLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := is.Left.EvaluateRow(row, input)
	val2 := is.Right.EvaluateRow(row, input)
	return storage.Is(val1, is.Left.toField().TP, val2, is.Right.toField().TP)
}

func (is IsLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if is.Left.AggrTypeCheck(groupByExpr) == nil && is.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if is.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", is))
}

func (is IsLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	is.Left.Accumulate(row, input)
	is.Right.Accumulate(row, input)
}

func (is IsLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := is.Left.AccumulateValue()
	rightAccumulateValue := is.Right.AccumulateValue()
	return storage.Is(leftAccumulateValue, is.Left.toField().TP, rightAccumulateValue,
		is.Right.toField().TP)
}

func (is IsLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return IsLogicExpr{
		Left:  is.Left.Clone(cloneAccumulator),
		Right: is.Right.Clone(cloneAccumulator),
		Name:  is.Name,
	}
}

func (is IsLogicExpr) HasGroupFunc() bool {
	return is.Left.HasGroupFunc() || is.Right.HasGroupFunc()
}

func (is IsLogicExpr) Compute() ([]byte, error) {
	val1, err := is.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := is.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Is(val1, is.Left.toField().TP, val2, is.Right.toField().TP), nil
}

type NotEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (notEqual NotEqualLogicExpr) toField() storage.Field {
	leftInputField := notEqual.Left.toField()
	rightInputField := notEqual.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.NotEqualOpType)
	f := storage.Field{Name: notEqual.String(), TP: tp}
	return f
}

func (notEqual NotEqualLogicExpr) String() string {
	return fmt.Sprintf("NotEqual(%s, %s)", notEqual.Left, notEqual.Right)
}
func (notEqual NotEqualLogicExpr) TypeCheck() error {
	err := notEqual.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = notEqual.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := notEqual.Left.toField()
	field2 := notEqual.Right.toField()
	return field1.CanOp(field2, storage.NotEqualOpType)
}

func (notEqual NotEqualLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if notEqual.Left.AggrTypeCheck(groupByExpr) == nil && notEqual.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if notEqual.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", notEqual))
}

func (notEqual NotEqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := notEqual.Left.Evaluate(input)
	rightColumnVector := notEqual.Right.Evaluate(input)
	return leftColumnVector.NotEqual(rightColumnVector, notEqual.String())
}

func (notEqual NotEqualLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := notEqual.Left.EvaluateRow(row, input)
	val2 := notEqual.Right.EvaluateRow(row, input)
	return storage.NotEqual(val1, notEqual.Left.toField().TP, val2, notEqual.Right.toField().TP)
}

func (notEqual NotEqualLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	notEqual.Left.Accumulate(row, input)
	notEqual.Right.Accumulate(row, input)
}

func (notEqual NotEqualLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := notEqual.Left.AccumulateValue()
	rightAccumulateValue := notEqual.Right.AccumulateValue()
	return storage.NotEqual(leftAccumulateValue, notEqual.Left.toField().TP, rightAccumulateValue,
		notEqual.Right.toField().TP)
}

func (notEqual NotEqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return NotEqualLogicExpr{
		Left:  notEqual.Left.Clone(cloneAccumulator),
		Right: notEqual.Right.Clone(cloneAccumulator),
		Name:  notEqual.Name,
	}
}

func (notEqual NotEqualLogicExpr) HasGroupFunc() bool {
	return notEqual.Left.HasGroupFunc() || notEqual.Right.HasGroupFunc()
}

func (notEqual NotEqualLogicExpr) Compute() ([]byte, error) {
	val1, err := notEqual.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := notEqual.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.NotEqual(val1, notEqual.Left.toField().TP, val2, notEqual.Right.toField().TP), nil
}

type GreatLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (great GreatLogicExpr) toField() storage.Field {
	leftInputField := great.Left.toField()
	rightInputField := great.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.GreatOpType)
	f := storage.Field{Name: great.String(), TP: tp}
	return f
}

func (great GreatLogicExpr) String() string {
	return fmt.Sprintf("Great(%s, %s)", great.Left, great.Right)
}

func (great GreatLogicExpr) TypeCheck() error {
	err := great.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = great.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := great.Left.toField()
	field2 := great.Right.toField()
	return field1.CanOp(field2, storage.GreatOpType)
}

func (great GreatLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := great.Left.Evaluate(input)
	rightColumnVector := great.Right.Evaluate(input)
	return leftColumnVector.Great(rightColumnVector, great.String())
}

func (great GreatLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := great.Left.EvaluateRow(row, input)
	val2 := great.Right.EvaluateRow(row, input)
	return storage.Great(val1, great.Left.toField().TP, val2, great.Right.toField().TP)
}

func (great GreatLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if great.Left.AggrTypeCheck(groupByExpr) == nil && great.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if great.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", great))
}

func (great GreatLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	great.Left.Accumulate(row, input)
	great.Right.Accumulate(row, input)
}

func (great GreatLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := great.Left.AccumulateValue()
	rightAccumulateValue := great.Right.AccumulateValue()
	return storage.Great(leftAccumulateValue, great.Left.toField().TP, rightAccumulateValue,
		great.Right.toField().TP)
}

func (great GreatLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return GreatLogicExpr{
		Left:  great.Left.Clone(cloneAccumulator),
		Right: great.Right.Clone(cloneAccumulator),
		Name:  great.Name,
	}
}

func (great GreatLogicExpr) HasGroupFunc() bool {
	return great.Left.HasGroupFunc() || great.Right.HasGroupFunc()
}

func (great GreatLogicExpr) Compute() ([]byte, error) {
	val1, err := great.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := great.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Great(val1, great.Left.toField().TP, val2, great.Right.toField().TP), nil
}

type GreatEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (greatEqual GreatEqualLogicExpr) toField() storage.Field {
	leftInputField := greatEqual.Left.toField()
	rightInputField := greatEqual.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.GreatEqualOpType)
	f := storage.Field{Name: greatEqual.String(), TP: tp}
	return f
}

func (greatEqual GreatEqualLogicExpr) String() string {
	return fmt.Sprintf("GreatEqual(%s, %s)", greatEqual.Left, greatEqual.Right)
}
func (greatEqual GreatEqualLogicExpr) TypeCheck() error {
	err := greatEqual.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = greatEqual.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := greatEqual.Left.toField()
	field2 := greatEqual.Right.toField()
	return field1.CanOp(field2, storage.GreatEqualOpType)
}

func (greatEqual GreatEqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := greatEqual.Left.Evaluate(input)
	rightColumnVector := greatEqual.Right.Evaluate(input)
	return leftColumnVector.GreatEqual(rightColumnVector, greatEqual.String())
}

func (greatEqual GreatEqualLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := greatEqual.Left.EvaluateRow(row, input)
	val2 := greatEqual.Right.EvaluateRow(row, input)
	return storage.GreatEqual(val1, greatEqual.Left.toField().TP, val2, greatEqual.Right.toField().TP)
}

func (greatEqual GreatEqualLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if greatEqual.Left.AggrTypeCheck(groupByExpr) == nil && greatEqual.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if greatEqual.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", greatEqual))
}

func (greatEqual GreatEqualLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	greatEqual.Left.Accumulate(row, input)
	greatEqual.Right.Accumulate(row, input)
}

func (greatEqual GreatEqualLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := greatEqual.Left.AccumulateValue()
	rightAccumulateValue := greatEqual.Right.AccumulateValue()
	return storage.GreatEqual(leftAccumulateValue, greatEqual.Left.toField().TP, rightAccumulateValue,
		greatEqual.Right.toField().TP)
}

func (greatEqual GreatEqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return GreatEqualLogicExpr{
		Left:  greatEqual.Left.Clone(cloneAccumulator),
		Right: greatEqual.Right.Clone(cloneAccumulator),
		Name:  greatEqual.Name,
	}
}

func (greatEqual GreatEqualLogicExpr) HasGroupFunc() bool {
	return greatEqual.Left.HasGroupFunc() || greatEqual.Right.HasGroupFunc()
}

func (greatEqual GreatEqualLogicExpr) Compute() ([]byte, error) {
	val1, err := greatEqual.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := greatEqual.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.GreatEqual(val1, greatEqual.Left.toField().TP, val2, greatEqual.Right.toField().TP), nil
}

type LessLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (less LessLogicExpr) toField() storage.Field {
	leftInputField := less.Left.toField()
	rightInputField := less.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.LessOpType)
	f := storage.Field{Name: less.String(), TP: tp}
	return f
}

func (less LessLogicExpr) String() string {
	return fmt.Sprintf("Less(%s, %s)", less.Left, less.Right)
}

func (less LessLogicExpr) TypeCheck() error {
	err := less.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = less.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := less.Left.toField()
	field2 := less.Right.toField()
	return field1.CanOp(field2, storage.LessOpType)
}

func (less LessLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := less.Left.Evaluate(input)
	rightColumnVector := less.Right.Evaluate(input)
	return leftColumnVector.Less(rightColumnVector, less.String())
}

func (less LessLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := less.Left.EvaluateRow(row, input)
	val2 := less.Right.EvaluateRow(row, input)
	return storage.Less(val1, less.Left.toField().TP, val2, less.Right.toField().TP)
}

func (less LessLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if less.Left.AggrTypeCheck(groupByExpr) == nil && less.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if less.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", less))
}

func (less LessLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	less.Left.Accumulate(row, input)
	less.Right.Accumulate(row, input)
}

func (less LessLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := less.Left.AccumulateValue()
	rightAccumulateValue := less.Right.AccumulateValue()
	return storage.Less(leftAccumulateValue, less.Left.toField().TP, rightAccumulateValue,
		less.Right.toField().TP)
}

func (less LessLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return LessLogicExpr{
		Left:  less.Left.Clone(cloneAccumulator),
		Right: less.Right.Clone(cloneAccumulator),
		Name:  less.Name,
	}
}

func (less LessLogicExpr) HasGroupFunc() bool {
	return less.Left.HasGroupFunc() || less.Right.HasGroupFunc()
}

func (less LessLogicExpr) Compute() ([]byte, error) {
	val1, err := less.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := less.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Less(val1, less.Left.toField().TP, val2, less.Right.toField().TP), nil
}

type LessEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (lessEqual LessEqualLogicExpr) toField() storage.Field {
	leftInputField := lessEqual.Left.toField()
	rightInputField := lessEqual.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.LessEqualOpType)
	f := storage.Field{Name: lessEqual.String(), TP: tp}
	return f
}
func (lessEqual LessEqualLogicExpr) String() string {
	return fmt.Sprintf("LessEqual(%s, %s)", lessEqual.Left, lessEqual.Right)
}
func (lessEqual LessEqualLogicExpr) TypeCheck() error {
	err := lessEqual.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = lessEqual.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := lessEqual.Left.toField()
	field2 := lessEqual.Right.toField()
	return field1.CanOp(field2, storage.LessEqualOpType)
}

func (lessEqual LessEqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := lessEqual.Left.Evaluate(input)
	rightColumnVector := lessEqual.Right.Evaluate(input)
	return leftColumnVector.LessEqual(rightColumnVector, lessEqual.String())
}

func (lessEqual LessEqualLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := lessEqual.Left.EvaluateRow(row, input)
	val2 := lessEqual.Right.EvaluateRow(row, input)
	return storage.LessEqual(val1, lessEqual.Left.toField().TP, val2, lessEqual.Right.toField().TP)
}

func (lessEqual LessEqualLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if lessEqual.Left.AggrTypeCheck(groupByExpr) == nil && lessEqual.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if lessEqual.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", lessEqual))
}

func (lessEqual LessEqualLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	lessEqual.Left.Accumulate(row, input)
	lessEqual.Right.Accumulate(row, input)
}

func (lessEqual LessEqualLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := lessEqual.Left.AccumulateValue()
	rightAccumulateValue := lessEqual.Right.AccumulateValue()
	return storage.LessEqual(leftAccumulateValue, lessEqual.Left.toField().TP, rightAccumulateValue,
		lessEqual.Right.toField().TP)
}

func (lessEqual LessEqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return LessEqualLogicExpr{
		Left:  lessEqual.Left.Clone(cloneAccumulator),
		Right: lessEqual.Right.Clone(cloneAccumulator),
		Name:  lessEqual.Name,
	}
}

func (lessEqual LessEqualLogicExpr) HasGroupFunc() bool {
	return lessEqual.Left.HasGroupFunc() || lessEqual.Right.HasGroupFunc()
}

func (lessEqual LessEqualLogicExpr) Compute() ([]byte, error) {
	val1, err := lessEqual.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := lessEqual.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.LessEqual(val1, lessEqual.Left.toField().TP, val2, lessEqual.Right.toField().TP), nil
}

type AndLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (and AndLogicExpr) toField() storage.Field {
	leftInputField := and.Left.toField()
	rightInputField := and.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.AndOpType)
	f := storage.Field{Name: and.String(), TP: tp}
	return f
}

func (and AndLogicExpr) String() string {
	return fmt.Sprintf("AND(%s, %s)", and.Left, and.Right)
}

func (and AndLogicExpr) TypeCheck() error {
	err := and.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = and.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := and.Left.toField()
	field2 := and.Right.toField()
	return field1.CanOp(field2, storage.AndOpType)
}

func (and AndLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := and.Left.Evaluate(input)
	rightColumnVector := and.Right.Evaluate(input)
	return leftColumnVector.And(rightColumnVector, and.String())
}

func (and AndLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := and.Left.EvaluateRow(row, input)
	val2 := and.Right.EvaluateRow(row, input)
	return storage.And(val1, val2)
}

func (and AndLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if and.Left.AggrTypeCheck(groupByExpr) == nil && and.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if and.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", and))
}

func (and AndLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	and.Left.Accumulate(row, input)
	and.Right.Accumulate(row, input)
}

func (and AndLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := and.Left.AccumulateValue()
	rightAccumulateValue := and.Right.AccumulateValue()
	return storage.And(leftAccumulateValue, rightAccumulateValue)
}

func (and AndLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return AndLogicExpr{
		Left:  and.Left.Clone(cloneAccumulator),
		Right: and.Right.Clone(cloneAccumulator),
		Name:  and.Name,
	}
}

func (and AndLogicExpr) Compute() ([]byte, error) {
	val1, err := and.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := and.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.And(val1, val2), nil
}

func (and AndLogicExpr) HasGroupFunc() bool {
	return and.Left.HasGroupFunc() || and.Right.HasGroupFunc()
}

type OrLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (or OrLogicExpr) toField() storage.Field {
	leftInputField := or.Left.toField()
	rightInputField := or.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.OrOpType)
	f := storage.Field{Name: or.String(), TP: tp}
	return f
}

func (or OrLogicExpr) String() string {
	return fmt.Sprintf("OR(%s, %s)", or.Left, or.Right)
}
func (or OrLogicExpr) TypeCheck() error {
	err := or.Left.TypeCheck()
	if err != nil {
		return err
	}
	err = or.Right.TypeCheck()
	if err != nil {
		return err
	}
	field1 := or.Left.toField()
	field2 := or.Right.toField()
	return field1.CanOp(field2, storage.OrOpType)
}

func (or OrLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := or.Left.Evaluate(input)
	rightColumnVector := or.Right.Evaluate(input)
	return leftColumnVector.Or(rightColumnVector, or.String())
}

func (or OrLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := or.Left.EvaluateRow(row, input)
	val2 := or.Right.EvaluateRow(row, input)
	return storage.Or(val1, val2)
}

func (or OrLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	if or.Left.AggrTypeCheck(groupByExpr) == nil && or.Right.AggrTypeCheck(groupByExpr) == nil {
		return nil
	}
	for _, expr := range groupByExpr {
		if or.String() == expr.String() {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%s doesn't match group by clause", or))
}

func (or OrLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	or.Left.Accumulate(row, input)
	or.Right.Accumulate(row, input)
}

func (or OrLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := or.Left.AccumulateValue()
	rightAccumulateValue := or.Right.AccumulateValue()
	return storage.Or(leftAccumulateValue, rightAccumulateValue)
}

func (or OrLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return OrLogicExpr{
		Left:  or.Left.Clone(cloneAccumulator),
		Right: or.Right.Clone(cloneAccumulator),
		Name:  or.Name,
	}
}

func (or OrLogicExpr) HasGroupFunc() bool {
	return or.Left.HasGroupFunc() || or.Right.HasGroupFunc()
}

func (or OrLogicExpr) Compute() ([]byte, error) {
	val1, err := or.Left.Compute()
	if err != nil {
		return nil, err
	}
	val2, err := or.Right.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Or(val1, val2), nil
}

type OrderByLogicExpr struct {
	expr []LogicExpr
	asc  []bool
}

func (orderBy OrderByLogicExpr) toField(input LogicPlan) storage.Field {
	return storage.Field{Name: "order", TP: storage.Int}
}

func (orderBy OrderByLogicExpr) String() string {
	buf := bytes.Buffer{}
	buf.WriteString("orderBy(")
	for i, expr := range orderBy.expr {
		asc := "asc"
		if !orderBy.asc[i] {
			asc = "desc"
		}
		buf.WriteString(fmt.Sprintf("[%s, %s]", expr, asc))
		if i != len(orderBy.expr)-1 {
			buf.WriteString(",")
		}
	}
	return buf.String()
}

func (orderBy OrderByLogicExpr) TypeCheck() error {
	// only numerical or string type can be orderBy, aka comparable type.
	for _, expr := range orderBy.expr {
		err := expr.TypeCheck()
		if err != nil {
			return err
		}
		// Todo: do we need this?
		//if !expr.toField(input).IsComparable() {
		//	return errors.New("order by cannot be applied to")
		//}
	}
	return nil
}

func (orderBy OrderByLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	found := false
	for _, orderByExpr := range orderBy.expr {
		if orderByExpr.AggrTypeCheck(groupByExpr) != nil {
			found = true
		}
	}
	if !found {
		return nil
	}
	for _, orderByExpr := range orderBy.expr {
		found := false
		for _, expr := range groupByExpr {
			if orderByExpr.String() == expr.String() {
				found = true
				break
			}
		}
		if !found {
			return errors.New(fmt.Sprintf("%s doesn't match group by clause", orderByExpr))
		}
	}
	return nil
}

// generate a columnVector which represent the order of row within the input.
// For example, let's say input is:
// user_id, shop_id
// 10, 1
// 09, 2
// 11, 3
// and the orderBy field is user_id, then the columnVector will be:
// order
// 1
// 0
// 2
// so the orderBy Plan can change the input to:
// 09, 2
// 10, 1
// 11, 3
func (orderBy OrderByLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	ret := storage.ColumnVector{Field: storage.Field{Name: "order", TP: storage.Int}}
	for i := 0; i < input.RowCount(); i++ {
		val := storage.EncodeInt(int64(i))
		ret.Values = append(ret.Values, val)
	}
	sortedVector := make([]storage.ColumnVector, len(orderBy.expr))
	asc := make([]bool, len(orderBy.expr))
	for i, expr := range orderBy.expr {
		columnVector := expr.Evaluate(input)
		sortedVector[i] = columnVector
		asc[i] = orderBy.asc[i]
	}
	return ret.Sort(sortedVector, asc)
}

func (orderBy OrderByLogicExpr) Compute() []byte {
	panic("unsupported method")
}

// For sql, there are some aggregation function and non aggregation function. We put all non aggregation function
// here.
type FuncCallLogicExpr struct {
	FuncName string
	Params   []LogicExpr
	Name     string
	Fn       FuncInterface
}

func (call FuncCallLogicExpr) toField() storage.Field {
	return storage.Field{Name: call.Name, TP: call.Fn.ReturnType()}
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

func (call FuncCallLogicExpr) TypeCheck() error {
	f := call.Fn
	if f == nil {
		return errors.New("no such func")
	}
	if len(call.Params) != f.FuncParamSize() {
		return errors.New("func param doesn't match")
	}
	paramFields := make([]storage.Field, len(call.Params))
	for i, param := range call.Params {
		err := param.TypeCheck()
		if err != nil {
			return err
		}
		paramFields[i] = param.toField()
	}
	return f.TypeCheck()
}

func (call FuncCallLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	f := call.Fn
	columnVectors := make([]storage.ColumnVector, len(call.Params))
	for i, param := range call.Params {
		columnVectors[i] = param.Evaluate(input)
	}
	ret := storage.ColumnVector{Field: storage.Field{Name: call.Name, TP: f.ReturnType()}}
	for i := 0; i < columnVectors[i].Size(); i++ {
		params := make([][]byte, len(columnVectors))
		for j, columnVector := range columnVectors {
			params[j] = columnVector.Values[i]
		}
		oneRecord := f.F()(params)
		ret.Values = append(ret.Values, oneRecord)
	}
	return ret
}

func (call FuncCallLogicExpr) IsAggrFunc() bool {
	return call.IsAggrFunc()
}

func (call FuncCallLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	// either param is a aggr function or the param is in group by.
	// and when a param is aggr function, it's param cannot be aggr function.
	// Todo: tricky?
	if call.IsAggrFunc() {
		for _, param := range call.Params {
			if param.HasGroupFunc() {
				return errors.New("invalid use of group function")
			}
		}
		return nil
	}
	// If it's not a aggr func
	// It's parameter must be either a groupFunc, or a column indicated on groupByExpr
	// Todo: what if param is like: sum(count())
	for _, param := range call.Params {
		if param.HasGroupFunc() {
			continue
		}
		err := param.AggrTypeCheck(groupByExpr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (call FuncCallLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	call.Fn.Accumulate(row, input)
}

func (call FuncCallLogicExpr) AccumulateValue() []byte {
	return call.Fn.AccumulateValue()
}

func (call FuncCallLogicExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	params := make([][]byte, len(call.Params))
	for i := 0; i < len(call.Params); i++ {
		params[i] = call.Params[i].EvaluateRow(row, input)
	}
	return call.Fn.F()(params)
}

func (call FuncCallLogicExpr) Clone(cloneAccumulate bool) LogicExpr {
	ret := FuncCallLogicExpr{
		FuncName: call.FuncName,
		Params:   make([]LogicExpr, len(call.Params)),
		Name:     call.Name,
	}
	for i, expr := range call.Params {
		call.Params[i] = expr.Clone(cloneAccumulate)
	}
	ret.Fn = getFunc(call.FuncName, call.Params)
	return ret
}

func (call FuncCallLogicExpr) Compute() ([]byte, error) {
	return nil, errors.New("unsupported method")
}

func (call FuncCallLogicExpr) HasGroupFunc() bool {
	if call.IsAggrFunc() {
		return true
	}
	for _, param := range call.Params {
		if param.HasGroupFunc() {
			return true
		}
	}
	return false
}

func MakeFuncCallLogicExpr(name string, params []LogicExpr) FuncCallLogicExpr {
	return FuncCallLogicExpr{
		FuncName: name,
		Name:     name,
		Fn:       getFunc(name, params),
		Params:   params,
	}
}

type AsLogicExpr struct {
	Expr  LogicExpr
	Alias string
}

func (as AsLogicExpr) toField() storage.Field {
	exprField := as.Expr.toField()
	f := storage.Field{Name: as.Alias, TP: exprField.TP}
	return f
}

func (as AsLogicExpr) String() string {
	return as.Alias
}
func (as AsLogicExpr) TypeCheck() error {
	return as.Expr.TypeCheck()
}

func (as AsLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	return as.Expr.Evaluate(input)
}
