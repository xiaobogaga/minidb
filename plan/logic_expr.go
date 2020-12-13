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
	AggrTypeCheck(groupByExpr []LogicExpr) error
	Evaluate(input *storage.RecordBatch) storage.ColumnVector
	Accumulate(row int, input *storage.RecordBatch) // Accumulate the value.
	AccumulateValue() []byte
	Clone(cloneAccumulator bool) LogicExpr
}

// can be a.b.c or a.b or a
type IdentifierLogicExpr struct {
	Ident  []byte
	Schema string
	Table  string
	Column string
	accumulator []byte // put accumulator here is not a good idea. It's better to seperate.
}

func (ident IdentifierLogicExpr) toField(input LogicPlan) storage.Field {
	// The column must be unique in the input schema.
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

func (ident IdentifierLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	return input.GetColumnValue(ident.Column)
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

type LiteralLogicExpr struct {
	TP   storage.FieldTP
	Data []byte
}

func (literal LiteralLogicExpr) toField(input LogicPlan) storage.Field {
	return storage.InferenceType(literal.Data)
}

func (literal LiteralLogicExpr) TypeCheck(input LogicPlan) error {
	return nil
}

func (literal LiteralLogicExpr) String() string {
	return string(literal.Data)
}

func (literal LiteralLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	ret := storage.ColumnVector{
		Field: storage.InferenceType(literal.Data),
	}
	for i := 0; i < input.RowCount(); i++ {
		ret.Values = append(ret.Values, literal.Data)
	}
	return ret
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

type NegativeLogicExpr struct {
	Input LogicExpr
	Name  string
	Alias string
}

func (negative NegativeLogicExpr) toField(input LogicPlan) storage.Field {
	field := negative.Input.toField(input)
	ret := storage.Field{
		TP:         field.TP,
		Name:       negative.String(),
		TableName:  field.TableName,
		SchemaName: field.SchemaName,
	}
	return ret
}

func (negative NegativeLogicExpr) TypeCheck(input LogicPlan) error {
	err := negative.Input.TypeCheck(input)
	if err != nil {
		return err
	}
	field := negative.Input.toField(input)
	if !field.IsFieldNumerialType() {
		return errors.New("- cannot applied to non numerical type")
	}
	return nil
}

func (negative NegativeLogicExpr) String() string {
	return fmt.Sprintf("-%s", negative.Input)
}

func (negative NegativeLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	columnVector := negative.Input.Evaluate(input)
	return columnVector.Negative()
}

func (negative NegativeLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	// It works for query:
	// select -id from mytest group by id
	// select -id from mytest group by -id
	err := negative.Input.AggrTypeCheck(groupByExpr)
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
	negative.Input.Accumulate(row, input)
}

func (negative NegativeLogicExpr) AccumulateValue() []byte {
	val := negative.Input.AccumulateValue()
	// Todo return -val
	return val
}

func (negative NegativeLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return NegativeLogicExpr{
		Input: negative.Input.Clone(cloneAccumulator),
		Name: negative.Name,
		Alias: negative.Alias,
	}
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
	tp := leftInputField.InferenceType(rightInputField, storage.AddOpType)
	f := storage.Field{Name: add.String(), TP: tp}
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
	if !field1.IsFieldNumerialType() || !field2.IsFieldNumerialType() {
		return errors.New("+ cannot applied to non numerical type")
	}
	return nil
}

func (add AddLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := add.Left.Evaluate(input)
	rightColumnVector := add.Right.Evaluate(input)
	return leftColumnVector.Add(rightColumnVector)
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
	// Todo
	return leftAccumulateValue
}

func (add AddLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return AddLogicExpr{
		Left: add.Left.Clone(cloneAccumulator),
		Right: add.Right.Clone(cloneAccumulator),
		Name: add.Name,
		Alias: add.Alias,
	}
}

type MinusLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (minus MinusLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := minus.Left.toField(input)
	rightInputField := minus.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.MinusOpType)
	f := storage.Field{Name: minus.String(), TP: tp}
	return f
}

func (minus MinusLogicExpr) String() string {
	return fmt.Sprintf("Minus(%s, %s)", minus.Left, minus.Right)
}

func (minus MinusLogicExpr) TypeCheck(input LogicPlan) error {
	err := minus.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = minus.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := minus.Left.toField(input)
	field2 := minus.Right.toField(input)
	if !field1.IsFieldNumerialType() || !field2.IsFieldNumerialType() {
		return errors.New("- cannot applied to non numerical type")
	}
	return nil
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
	return leftColumnVector.Minus(rightColumnVector)
}

func (minus MinusLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	minus.Left.Accumulate(row, input)
	minus.Right.Accumulate(row, input)
}

func (minus MinusLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := minus.Left.AccumulateValue()
	rightAccumulateValue := minus.Right.AccumulateValue()
	// Todo
	return leftAccumulateValue
}

func (minus MinusLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return MinusLogicExpr{
		Left:  minus.Left.Clone(cloneAccumulator),
		Right: minus.Right.Clone(cloneAccumulator),
		Name:  minus.Name,
		Alias: minus.Alias,
	}
}

type MulLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (mul MulLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := mul.Left.toField(input)
	rightInputField := mul.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.MulOpType)
	f := storage.Field{Name: mul.String(), TP: tp}
	return f
}

func (mul MulLogicExpr) String() string {
	return fmt.Sprintf("Mul(%s, %s)", mul.Left, mul.Right)
}

func (mul MulLogicExpr) TypeCheck(input LogicPlan) error {
	err := mul.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = mul.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := mul.Left.toField(input)
	field2 := mul.Right.toField(input)
	if !field1.IsFieldNumerialType() || !field2.IsFieldNumerialType() {
		return errors.New("* cannot applied to non numerical type")
	}
	return nil
}

func (mul MulLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := mul.Left.Evaluate(input)
	rightColumnVector := mul.Right.Evaluate(input)
	return leftColumnVector.Mul(rightColumnVector)
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
	// Todo
	return leftAccumulateValue
}

func (mul MulLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return MulLogicExpr{
		Left:  mul.Left.Clone(cloneAccumulator),
		Right: mul.Right.Clone(cloneAccumulator),
		Name:  mul.Name,
		Alias: mul.Alias,
	}
}

type DivideLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
	Alias string
}

func (divide DivideLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := divide.Left.toField(input)
	rightInputField := divide.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.DivideOpType)
	f := storage.Field{Name: divide.String(), TP: tp}
	return f
}

func (divide DivideLogicExpr) String() string {
	return fmt.Sprintf("Divide(%s, %s)", divide.Left, divide.Right)
}

func (divide DivideLogicExpr) TypeCheck(input LogicPlan) error {
	err := divide.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = divide.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := divide.Left.toField(input)
	field2 := divide.Right.toField(input)
	if !field1.IsFieldNumerialType() || !field2.IsFieldNumerialType() {
		return errors.New("/ cannot applied to non numerical type")
	}
	return nil
}

func (divide DivideLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := divide.Left.Evaluate(input)
	rightColumnVector := divide.Right.Evaluate(input)
	return leftColumnVector.Divide(rightColumnVector)
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
	// Todo
	return leftAccumulateValue
}

func (divide DivideLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return DivideLogicExpr{
		Left:  divide.Left.Clone(cloneAccumulator),
		Right: divide.Right.Clone(cloneAccumulator),
		Name:  divide.Name,
		Alias: divide.Alias,
	}
}

type ModLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (mod ModLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := mod.Left.toField(input)
	rightInputField := mod.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.ModOpType)
	f := storage.Field{Name: mod.String(), TP: tp}
	return f
}

func (mod ModLogicExpr) String() string {
	return fmt.Sprintf("Mod(%s, %s)", mod.Left, mod.Right)
}
func (mod ModLogicExpr) TypeCheck(input LogicPlan) error {
	err := mod.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = mod.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := mod.Left.toField(input)
	field2 := mod.Right.toField(input)
	if !field1.IsFieldNumerialType() || !field2.IsFieldNumerialType() {
		return errors.New("% cannot applied to non numerical type")
	}
	return nil
}

func (mod ModLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := mod.Left.Evaluate(input)
	rightColumnVector := mod.Right.Evaluate(input)
	return leftColumnVector.Mod(rightColumnVector)
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
}

func (mod ModLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return ModLogicExpr{
		Left:  mod.Left.Clone(cloneAccumulator),
		Right: mod.Right.Clone(cloneAccumulator),
		Name:  mod.Name,
	}
}

type EqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (equal EqualLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := equal.Left.toField(input)
	rightInputField := equal.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.EqualOpType)
	f := storage.Field{Name: equal.String(), TP: tp}
	return f
}

func (equal EqualLogicExpr) String() string {
	return fmt.Sprintf("Equal(%s, %s)", equal.Left, equal.Right)
}

func (equal EqualLogicExpr) TypeCheck(input LogicPlan) error {
	err := equal.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = equal.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := equal.Left.toField(input)
	field2 := equal.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanEqual(field2) {
		return errors.New("= cannot be applied to")
	}
	return nil
}

func (equal EqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := equal.Left.Evaluate(input)
	rightColumnVector := equal.Right.Evaluate(input)
	return leftColumnVector.Equal(rightColumnVector)
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
	// Todo
	return leftAccumulateValue
}

func (equal EqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return EqualLogicExpr{
		Left:  equal.Left.Clone(cloneAccumulator),
		Right: equal.Right.Clone(cloneAccumulator),
		Name:  equal.Name,
	}
}

type IsLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (is IsLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := is.Left.toField(input)
	rightInputField := is.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.IsOpType)
	f := storage.Field{Name: is.String(), TP: tp}
	return f
}

func (is IsLogicExpr) String() string {
	return fmt.Sprintf("IS(%s, %s)", is.Left, is.Right)
}
func (is IsLogicExpr) TypeCheck(input LogicPlan) error {
	err := is.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = is.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := is.Left.toField(input)
	field2 := is.Right.toField(input)
	if !field1.CanEqual(field2) {
		return errors.New("IS cannot be applied to")
	}
	return nil
}

func (is IsLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := is.Left.Evaluate(input)
	rightColumnVector := is.Right.Evaluate(input)
	return leftColumnVector.Is(rightColumnVector)
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
	// Todo
	return leftAccumulateValue
}

func (is IsLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return IsLogicExpr{
		Left:  is.Left.Clone(cloneAccumulator),
		Right: is.Right.Clone(cloneAccumulator),
		Name:  is.Name,
	}
}

type NotEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (notEqual NotEqualLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := notEqual.Left.toField(input)
	rightInputField := notEqual.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.NotEqualOpType)
	f := storage.Field{Name: notEqual.String(), TP: tp}
	return f
}

func (notEqual NotEqualLogicExpr) String() string {
	return fmt.Sprintf("NotEqual(%s, %s)", notEqual.Left, notEqual.Right)
}
func (notEqual NotEqualLogicExpr) TypeCheck(input LogicPlan) error {
	err := notEqual.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = notEqual.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := notEqual.Left.toField(input)
	field2 := notEqual.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanEqual(field2) {
		return errors.New("!= cannot be applied to")
	}
	return nil
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
	return leftColumnVector.NotEqual(rightColumnVector)
}

func (notEqual NotEqualLogicExpr) Accumulate(row int, input *storage.RecordBatch) {
	notEqual.Left.Accumulate(row, input)
	notEqual.Right.Accumulate(row, input)
}

func (notEqual NotEqualLogicExpr) AccumulateValue() []byte {
	leftAccumulateValue := notEqual.Left.AccumulateValue()
	rightAccumulateValue := notEqual.Right.AccumulateValue()
}

func (notEqual NotEqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return NotEqualLogicExpr{
		Left:  notEqual.Left.Clone(cloneAccumulator),
		Right: notEqual.Right.Clone(cloneAccumulator),
		Name:  notEqual.Name,
	}
}

type GreatLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (great GreatLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := great.Left.toField(input)
	rightInputField := great.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.GreatOpType)
	f := storage.Field{Name: great.String(), TP: tp}
	return f
}

func (great GreatLogicExpr) String() string {
	return fmt.Sprintf("Great(%s, %s)", great.Left, great.Right)
}

func (great GreatLogicExpr) TypeCheck(input LogicPlan) error {
	err := great.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = great.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := great.Left.toField(input)
	field2 := great.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanCompare(field2) {
		return errors.New("> cannot be applied to")
	}
	return nil
}

func (great GreatLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := great.Left.Evaluate(input)
	rightColumnVector := great.Right.Evaluate(input)
	return leftColumnVector.Great(rightColumnVector)
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
}

func (great GreatLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return GreatLogicExpr{
		Left:  great.Left.Clone(cloneAccumulator),
		Right: great.Right.Clone(cloneAccumulator),
		Name:  great.Name,
	}
}

type GreatEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (greatEqual GreatEqualLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := greatEqual.Left.toField(input)
	rightInputField := greatEqual.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.GreatEqualOpType)
	f := storage.Field{Name: greatEqual.String(), TP: tp}
	return f
}

func (greatEqual GreatEqualLogicExpr) String() string {
	return fmt.Sprintf("GreatEqual(%s, %s)", greatEqual.Left, greatEqual.Right)
}
func (greatEqual GreatEqualLogicExpr) TypeCheck(input LogicPlan) error {
	err := greatEqual.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = greatEqual.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := greatEqual.Left.toField(input)
	field2 := greatEqual.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanCompare(field2) {
		return errors.New(">= cannot be applied to")
	}
	return nil
}

func (greatEqual GreatEqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := greatEqual.Left.Evaluate(input)
	rightColumnVector := greatEqual.Right.Evaluate(input)
	return leftColumnVector.GreatEqual(rightColumnVector)
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
}

func (greatEqual GreatEqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return GreatEqualLogicExpr{
		Left:  greatEqual.Left.Clone(cloneAccumulator),
		Right: greatEqual.Right.Clone(cloneAccumulator),
		Name:  greatEqual.Name,
	}
}

type LessLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (less LessLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := less.Left.toField(input)
	rightInputField := less.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.LessOpType)
	f := storage.Field{Name: less.String(), TP: tp}
	return f
}

func (less LessLogicExpr) String() string {
	return fmt.Sprintf("Less(%s, %s)", less.Left, less.Right)
}

func (less LessLogicExpr) TypeCheck(input LogicPlan) error {
	err := less.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = less.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := less.Left.toField(input)
	field2 := less.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanCompare(field2) {
		return errors.New("< cannot be applied to")
	}
	return nil
}

func (less LessLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := less.Left.Evaluate(input)
	rightColumnVector := less.Right.Evaluate(input)
	return leftColumnVector.Less(rightColumnVector)
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
	less.Left.AccumulateValue()
	less.Right.AccumulateValue()
}

func (less LessLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return LessLogicExpr{
		Left:  less.Left.Clone(cloneAccumulator),
		Right: less.Right.Clone(cloneAccumulator),
		Name:  less.Name,
	}
}

type LessEqualLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (lessEqual LessEqualLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := lessEqual.Left.toField(input)
	rightInputField := lessEqual.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.LessEqualOpType)
	f := storage.Field{Name: lessEqual.String(), TP: tp}
	return f
}
func (lessEqual LessEqualLogicExpr) String() string {
	return fmt.Sprintf("LessEqual(%s, %s)", lessEqual.Left, lessEqual.Right)
}
func (lessEqual LessEqualLogicExpr) TypeCheck(input LogicPlan) error {
	err := lessEqual.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = lessEqual.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := lessEqual.Left.toField(input)
	field2 := lessEqual.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanCompare(field2) {
		return errors.New("<= cannot be applied to")
	}
	return nil
}

func (lessEqual LessEqualLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := lessEqual.Left.Evaluate(input)
	rightColumnVector := lessEqual.Right.Evaluate(input)
	return leftColumnVector.LessEqual(rightColumnVector)
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
}

func (lessEqual LessEqualLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return LessEqualLogicExpr{
		Left:  lessEqual.Left.Clone(cloneAccumulator),
		Right: lessEqual.Right.Clone(cloneAccumulator),
		Name:  lessEqual.Name,
	}
}

type AndLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (and AndLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := and.Left.toField(input)
	rightInputField := and.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.AndOpType)
	f := storage.Field{Name: and.String(), TP: tp}
	return f
}

func (and AndLogicExpr) String() string {
	return fmt.Sprintf("AND(%s, %s)", and.Left, and.Right)
}

func (and AndLogicExpr) TypeCheck(input LogicPlan) error {
	err := and.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = and.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := and.Left.toField(input)
	field2 := and.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanLogicOp(field2) {
		return errors.New("and(&&) cannot be applied to")
	}
	return nil
}

func (and AndLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := and.Left.Evaluate(input)
	rightColumnVector := and.Right.Evaluate(input)
	return leftColumnVector.And(rightColumnVector)
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
}

func (and AndLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return AndLogicExpr{
		Left:  and.Left.Clone(cloneAccumulator),
		Right: and.Right.Clone(cloneAccumulator),
		Name:  and.Name,
	}
}

type OrLogicExpr struct {
	Left  LogicExpr
	Right LogicExpr
	Name  string
}

func (or OrLogicExpr) toField(input LogicPlan) storage.Field {
	leftInputField := or.Left.toField(input)
	rightInputField := or.Right.toField(input)
	tp := leftInputField.InferenceType(rightInputField, storage.OrOpType)
	f := storage.Field{Name: or.String(), TP: tp}
	return f
}

func (or OrLogicExpr) String() string {
	return fmt.Sprintf("OR(%s, %s)", or.Left, or.Right)
}
func (or OrLogicExpr) TypeCheck(input LogicPlan) error {
	err := or.Left.TypeCheck(input)
	if err != nil {
		return err
	}
	err = or.Right.TypeCheck(input)
	if err != nil {
		return err
	}
	field1 := or.Left.toField(input)
	field2 := or.Right.toField(input)
	// Check whether field2 can be equal compare with field2.
	if !field1.CanLogicOp(field2) {
		return errors.New("or(||) cannot be applied to")
	}
	return nil
}

func (or OrLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	leftColumnVector := or.Left.Evaluate(input)
	rightColumnVector := or.Right.Evaluate(input)
	return leftColumnVector.Or(rightColumnVector)
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
}
func (or OrLogicExpr) Clone(cloneAccumulator bool) LogicExpr {
	return OrLogicExpr{
		Left:  or.Left.Clone(cloneAccumulator),
		Right: or.Right.Clone(cloneAccumulator),
		Name:  or.Name,
	}
}

type OrderedLogicExpr struct {
	expr []LogicExpr
	asc  []bool
}

func (orderBy OrderedLogicExpr) toField(input LogicPlan) storage.Field {
	return storage.Field{Name: "order", TP: storage.Int}
}

func (orderBy OrderedLogicExpr) String() string {
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

func (orderBy OrderedLogicExpr) TypeCheck(input LogicPlan) error {
	// only numerical or string type can be orderBy, aka comparable type.
	for _, expr := range orderBy.expr {
		err := expr.TypeCheck(input)
		if err != nil {
			return err
		}
		if !expr.toField(input).IsComparable() {
			return errors.New("order by cannot be applied to")
		}
	}
	return nil
}

func (orderBy OrderedLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
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
func (orderBy OrderedLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	ret := storage.ColumnVector{Field: storage.Field{Name: "order", TP: storage.Int}}
	for i := 0; i < input.RowCount(); i++ {
		val, _ := storage.Encode(storage.Int, i)
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

// For sql, there are some aggregation function and non aggregation function. We put all non aggregation function
// here.
type FuncCallLogicExpr struct {
	FuncName string
	Params   []LogicExpr
	Name     string
	Func     FuncInterface
}

func (call FuncCallLogicExpr) toField(input LogicPlan) storage.Field {
	f := FuncCallMap[call.Name]
	return storage.Field{Name: call.Name, TP: f.ReturnType()}
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

func (call FuncCallLogicExpr) TypeCheck(input LogicPlan) error {
	f, ok := FuncCallMap[call.Name]
	if !ok {
		return errors.New("no such func supported")
	}
	call.Func = f
	if len(call.Params) != f.FuncParamSize() {
		return errors.New("func param doesn't match")
	}
	paramFields := make([]storage.Field, len(call.Params))
	for i, param := range call.Params {
		err := param.TypeCheck(input)
		if err != nil {
			return err
		}
		paramFields[i] = param.toField(input)
	}
	return f.TypeCheck(paramFields)
}

func (call FuncCallLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	f := FuncCallMap[call.Name]
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
		oneRecord := call.Func.F()(params)
		ret.Values = append(ret.Values, oneRecord)
	}
	return ret
}

func (call FuncCallLogicExpr) IsAggrFunc() bool {}

func (call FuncCallLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {

}

type AsLogicExpr struct {
	Expr  LogicExpr
	Alias string
}

func (as AsLogicExpr) toField(input LogicPlan) storage.Field {
	exprField := as.Expr.toField(input)
	f := storage.Field{Name: as.Alias, TP: exprField.TP}
	return f
}

func (as AsLogicExpr) String() string {
	return as.Alias
}
func (as AsLogicExpr) TypeCheck(input LogicPlan) error {
	return as.Expr.TypeCheck(input)
}

func (as AsLogicExpr) Evaluate(input *storage.RecordBatch) storage.ColumnVector {
	return as.Expr.Evaluate(input)
}

func (as AsLogicExpr) AggrTypeCheck(groupByExpr []LogicExpr) error {
	return as.Expr.AggrTypeCheck(groupByExpr)
}
