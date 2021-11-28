package plan

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/storage"
	"github.com/xiaobogaga/minidb/util"
)

type Expr interface {
	toField() storage.Field
	String() string
	TypeCheck() error
	AggrTypeCheck(groupByExpr []Expr) error
	Evaluate(input *storage.RecordBatch) *storage.ColumnVector
	EvaluateRow(row int, input *storage.RecordBatch) []byte
	Accumulate(row int, input *storage.RecordBatch) // Accumulate the value.
	AccumulateValue() []byte
	Clone(cloneAccumulator bool) Expr
	HasGroupFunc() bool
	Compute() ([]byte, error) // For insert, update, delete.
}

// can be a.b.c or a.b or a
type IdentifierExpr struct {
	Ident       []byte
	Accumulator []byte // put Accumulator here is not a good idea. It's better to separate.
	input       Plan
	Str         string // For debug only
}

func (ident *IdentifierExpr) toField() storage.Field {
	// The column must be unique in the input schema.
	schema := ident.input.Schema()
	databaseName, tableName, columnName := getSchemaTableColumnName(string(ident.Ident))
	return *schema.GetField(databaseName, tableName, columnName)
}

func (ident *IdentifierExpr) String() string {
	return string(ident.Ident)
}

func (ident *IdentifierExpr) TypeCheck() error {
	schemaName, table, column := getSchemaTableColumnName(string(ident.Ident))
	schema := ident.input.Schema()
	// Now we check whether we can find such column.
	if !schema.HasColumn(schemaName, table, column) {
		return errors.New(fmt.Sprintf("column '%s' cannot find", util.BuildDotString(schemaName, table, column)))
	}
	if schema.HasAmbiguousColumn(schemaName, table, column) {
		return errors.New(fmt.Sprintf("column '%s' is ambiguous", util.BuildDotString(schemaName, table, column)))
	}
	return nil
}

func (ident *IdentifierExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	schemaName, tableName, columnName := getSchemaTableColumnName(string(ident.Ident))
	return input.GetColumnValue(schemaName, tableName, columnName)
}

func (ident *IdentifierExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	schemaName, tableName, columnName := getSchemaTableColumnName(string(ident.Ident))
	return input.GetColumnValue(schemaName, tableName, columnName).RawValue(row)
}

func (ident *IdentifierExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (ident *IdentifierExpr) Clone(cloneAccumulate bool) Expr {
	ret := &IdentifierExpr{Ident: ident.Ident, Str: string(ident.Ident), input: ident.input}
	if cloneAccumulate {
		ret.Accumulator = ident.Accumulator
	}
	return ret
}

func (ident *IdentifierExpr) Accumulate(row int, input *storage.RecordBatch) {
	schemaName, tableName, columnName := getSchemaTableColumnName(string(ident.Ident))
	col := input.GetColumnValue(schemaName, tableName, columnName)
	ident.Accumulator = col.RawValue(row)
}

func (ident *IdentifierExpr) AccumulateValue() []byte {
	return ident.Accumulator
}

func (ident *IdentifierExpr) HasGroupFunc() bool { return false }

func (ident *IdentifierExpr) Compute() ([]byte, error) {
	return nil, errors.New("unsupported action")
}

type AllExpr struct {
	input Plan
	Str   string
}

func (all *AllExpr) toField() storage.Field {
	schema := all.input.Schema()
	return storage.Field{
		Name:       "*",
		TableName:  schema.TableName(),
		SchemaName: schema.SchemaName(),
		TP:         storage.FieldTP{Name: storage.Multiple},
	}
}

func (all *AllExpr) String() string {
	return "*"
}

func (all *AllExpr) TypeCheck() error {
	return all.input.TypeCheck()
}

func (all *AllExpr) AggrTypeCheck(_ []Expr) error {
	return errors.New("cannot group by on *")
}

func (all *AllExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	return input.Records[0]
}

func (all *AllExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	return input.Records[0].RawValue(row)
}

func (all *AllExpr) Accumulate(_ int, _ *storage.RecordBatch) {
	return
}

func (all *AllExpr) AccumulateValue() []byte {
	return nil
}

func (all *AllExpr) Clone(_ bool) Expr {
	all.input.Reset()
	return &AllExpr{input: all.input, Str: "*"}
}

func (all *AllExpr) HasGroupFunc() bool { return false }

func (all *AllExpr) Compute() ([]byte, error) { return nil, errors.New("unsupported action") }

type LiteralExpr struct {
	TP storage.FieldTP
	// Data is a bytes array, which might be a "xxx", or 'xxx' or true, false, or numerical value such as .10100, 01001, 909008
	// when we inference the type of data, it can be a numerical, bool, string, datetime, blob.
	// Numerical, bool, string can be easily fixed.
	// datetime must be a string start with '2020-10-12 10-13-14', or '2020-10-12 10-13-14' etc.
	// In mysql, some literal value can be transformed to another type, for example, true can be trans to integer 1.
	// But we will use a strict type. True cannot be transformed to 1.
	// For Blob, they must be a string 'xxx' or "xxx".
	Data []byte
	Str  string // For debug only
}

func (literal LiteralExpr) toField() storage.Field {
	f := storage.Field{Name: string(literal.Data)}
	f.TP = storage.InferenceType(literal.Data)
	return f
}

func (literal LiteralExpr) TypeCheck() error {
	return nil
}

func (literal LiteralExpr) String() string {
	return string(literal.Data)
}

func (literal LiteralExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	ret := &storage.ColumnVector{
		Field: storage.Field{Name: string(literal.Data), TP: storage.InferenceType(literal.Data)},
	}
	for i := 0; i < input.RowCount(); i++ {
		ret.Append(literal.Value())
	}
	return ret
}

func (literal LiteralExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	return literal.Value()
}

func (literal LiteralExpr) AggrTypeCheck(groupByExpr []Expr) error {
	return nil
}

func (literal LiteralExpr) Accumulate(row int, input *storage.RecordBatch) {
	return
}

func (literal LiteralExpr) AccumulateValue() []byte {
	return literal.Value()
}

func (literal LiteralExpr) Clone(cloneAccumulator bool) Expr {
	return literal
}

func (literal LiteralExpr) HasGroupFunc() bool { return false }

func (literal LiteralExpr) Compute() ([]byte, error) {
	return literal.Value(), nil
}

// a little tricky.
func (literal LiteralExpr) Value() []byte {
	return storage.Encode(literal.Data)
}

type NegativeExpr struct {
	Expr Expr
	Name string
}

func (negative NegativeExpr) toField() storage.Field {
	field := negative.Expr.toField()
	ret := storage.Field{
		TP:         field.TP,
		Name:       negative.String(),
		TableName:  field.TableName,
		SchemaName: field.SchemaName,
	}
	return ret
}

func (negative NegativeExpr) TypeCheck() error {
	err := negative.Expr.TypeCheck()
	if err != nil {
		return err
	}
	field := negative.Expr.toField()
	return field.CanOp(field, storage.NegativeOpType)
}

func (negative NegativeExpr) String() string {
	return fmt.Sprintf("-%s", negative.Expr)
}

func (negative NegativeExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	columnVector := negative.Expr.Evaluate(input)
	return columnVector.Negative()
}

func (negative NegativeExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	data := negative.Expr.EvaluateRow(row, input)
	return storage.Negative(negative.toField().TP, data)
}

func (negative NegativeExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (negative NegativeExpr) Accumulate(row int, input *storage.RecordBatch) {
	negative.Expr.Accumulate(row, input)
}

func (negative NegativeExpr) AccumulateValue() []byte {
	val := negative.Expr.AccumulateValue()
	return storage.Negative(negative.toField().TP, val)
}

func (negative NegativeExpr) Clone(cloneAccumulator bool) Expr {
	return NegativeExpr{
		Expr: negative.Expr.Clone(cloneAccumulator),
		Name: negative.Name,
	}
}

func (negative NegativeExpr) HasGroupFunc() bool {
	return negative.Expr.HasGroupFunc()
}

func (negative NegativeExpr) Compute() ([]byte, error) {
	val, err := negative.Expr.Compute()
	if err != nil {
		return nil, err
	}
	return storage.Negative(negative.toField().TP, val), nil
}

// Math Expr
type AddExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (add AddExpr) toField() storage.Field {
	leftInputField := add.Left.toField()
	rightInputField := add.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.AddOpType)
	f := storage.Field{Name: add.String(), TP: tp}
	return f
}

func (add AddExpr) String() string {
	return fmt.Sprintf("%s + %s", add.Left, add.Right)
}

func (add AddExpr) TypeCheck() error {
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

func (add AddExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := add.Left.Evaluate(input)
	rightColumnVector := add.Right.Evaluate(input)
	return leftColumnVector.Add(rightColumnVector, add.String())
}

func (add AddExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := add.Left.EvaluateRow(row, input)
	val2 := add.Right.EvaluateRow(row, input)
	return storage.Add(val1, add.Left.toField().TP, val2, add.Right.toField().TP)
}

func (add AddExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (add AddExpr) Accumulate(row int, input *storage.RecordBatch) {
	add.Left.Accumulate(row, input)
	add.Right.Accumulate(row, input)
}

func (add AddExpr) AccumulateValue() []byte {
	leftAccumulateValue := add.Left.AccumulateValue()
	rightAccumulateValue := add.Right.AccumulateValue()
	return storage.Add(leftAccumulateValue, add.Left.toField().TP, rightAccumulateValue,
		add.Right.toField().TP)
}

func (add AddExpr) Clone(cloneAccumulator bool) Expr {
	return AddExpr{
		Left:  add.Left.Clone(cloneAccumulator),
		Right: add.Right.Clone(cloneAccumulator),
		Name:  add.Name,
	}
}

func (add AddExpr) HasGroupFunc() bool {
	return add.Left.HasGroupFunc() || add.Right.HasGroupFunc()
}

func (add AddExpr) Compute() ([]byte, error) {
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

type MinusExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (minus MinusExpr) toField() storage.Field {
	leftInputField := minus.Left.toField()
	rightInputField := minus.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.MinusOpType)
	f := storage.Field{Name: minus.String(), TP: tp}
	return f
}

func (minus MinusExpr) String() string {
	return fmt.Sprintf("%s = %s", minus.Left, minus.Right)
}

func (minus MinusExpr) TypeCheck() error {
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

func (minus MinusExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (minus MinusExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := minus.Left.Evaluate(input)
	rightColumnVector := minus.Right.Evaluate(input)
	return leftColumnVector.Minus(rightColumnVector, minus.String())
}

func (minus MinusExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := minus.Left.EvaluateRow(row, input)
	val2 := minus.Right.EvaluateRow(row, input)
	return storage.Minus(val1, minus.Left.toField().TP, val2, minus.Right.toField().TP)
}

func (minus MinusExpr) Accumulate(row int, input *storage.RecordBatch) {
	minus.Left.Accumulate(row, input)
	minus.Right.Accumulate(row, input)
}

func (minus MinusExpr) AccumulateValue() []byte {
	leftAccumulateValue := minus.Left.AccumulateValue()
	rightAccumulateValue := minus.Right.AccumulateValue()
	return storage.Minus(leftAccumulateValue, minus.Left.toField().TP, rightAccumulateValue,
		minus.Right.toField().TP)
}

func (minus MinusExpr) Clone(cloneAccumulator bool) Expr {
	return MinusExpr{
		Left:  minus.Left.Clone(cloneAccumulator),
		Right: minus.Right.Clone(cloneAccumulator),
		Name:  minus.Name,
	}
}

func (minus MinusExpr) HasGroupFunc() bool {
	return minus.Left.HasGroupFunc() || minus.Right.HasGroupFunc()
}

func (minus MinusExpr) Compute() ([]byte, error) {
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

type MulExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (mul MulExpr) toField() storage.Field {
	leftInputField := mul.Left.toField()
	rightInputField := mul.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.MulOpType)
	f := storage.Field{Name: mul.String(), TP: tp}
	return f
}

func (mul MulExpr) String() string {
	return fmt.Sprintf("%s * %s", mul.Left, mul.Right)
}

func (mul MulExpr) TypeCheck() error {
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

func (mul MulExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := mul.Left.Evaluate(input)
	rightColumnVector := mul.Right.Evaluate(input)
	return leftColumnVector.Mul(rightColumnVector, mul.String())
}

func (mul MulExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := mul.Left.EvaluateRow(row, input)
	val2 := mul.Right.EvaluateRow(row, input)
	return storage.Mul(val1, mul.Left.toField().TP, val2, mul.Right.toField().TP)
}

func (mul MulExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (mul MulExpr) Accumulate(row int, input *storage.RecordBatch) {
	mul.Left.Accumulate(row, input)
	mul.Right.Accumulate(row, input)
}

func (mul MulExpr) AccumulateValue() []byte {
	leftAccumulateValue := mul.Left.AccumulateValue()
	rightAccumulateValue := mul.Right.AccumulateValue()
	return storage.Mul(leftAccumulateValue, mul.Left.toField().TP, rightAccumulateValue,
		mul.Right.toField().TP)
}

func (mul MulExpr) Clone(cloneAccumulator bool) Expr {
	return MulExpr{
		Left:  mul.Left.Clone(cloneAccumulator),
		Right: mul.Right.Clone(cloneAccumulator),
		Name:  mul.Name,
	}
}

func (mul MulExpr) HasGroupFunc() bool {
	return mul.Left.HasGroupFunc() || mul.Right.HasGroupFunc()
}

func (mul MulExpr) Compute() ([]byte, error) {
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

type DivideExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (divide DivideExpr) toField() storage.Field {
	leftInputField := divide.Left.toField()
	rightInputField := divide.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.DivideOpType)
	f := storage.Field{Name: divide.String(), TP: tp}
	return f
}

func (divide DivideExpr) String() string {
	return fmt.Sprintf("%s / %s", divide.Left, divide.Right)
}

func (divide DivideExpr) TypeCheck() error {
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

func (divide DivideExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := divide.Left.Evaluate(input)
	rightColumnVector := divide.Right.Evaluate(input)
	return leftColumnVector.Divide(rightColumnVector, divide.String())
}

func (divide DivideExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := divide.Left.EvaluateRow(row, input)
	val2 := divide.Right.EvaluateRow(row, input)
	return storage.Divide(val1, divide.Left.toField().TP, val2, divide.Right.toField().TP)
}

func (divide DivideExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (divide DivideExpr) Accumulate(row int, input *storage.RecordBatch) {
	divide.Left.Accumulate(row, input)
	divide.Right.Accumulate(row, input)
}

func (divide DivideExpr) AccumulateValue() []byte {
	leftAccumulateValue := divide.Left.AccumulateValue()
	rightAccumulateValue := divide.Right.AccumulateValue()
	return storage.Divide(leftAccumulateValue, divide.Left.toField().TP, rightAccumulateValue,
		divide.Right.toField().TP)
}

func (divide DivideExpr) Clone(cloneAccumulator bool) Expr {
	return DivideExpr{
		Left:  divide.Left.Clone(cloneAccumulator),
		Right: divide.Right.Clone(cloneAccumulator),
		Name:  divide.Name,
	}
}

func (divide DivideExpr) HasGroupFunc() bool {
	return divide.Left.HasGroupFunc() || divide.Right.HasGroupFunc()
}

func (divide DivideExpr) Compute() ([]byte, error) {
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

type ModExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (mod ModExpr) toField() storage.Field {
	leftInputField := mod.Left.toField()
	rightInputField := mod.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.ModOpType)
	f := storage.Field{Name: mod.String(), TP: tp}
	return f
}

func (mod ModExpr) String() string {
	return fmt.Sprintf("%s %s %s", mod.Left, "%", mod.Right)
}

func (mod ModExpr) TypeCheck() error {
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

func (mod ModExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := mod.Left.Evaluate(input)
	rightColumnVector := mod.Right.Evaluate(input)
	return leftColumnVector.Mod(rightColumnVector, mod.String())
}

func (mod ModExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := mod.Left.EvaluateRow(row, input)
	val2 := mod.Right.EvaluateRow(row, input)
	return storage.Mod(val1, mod.Left.toField().TP, val2, mod.Right.toField().TP)
}

func (mod ModExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (mod ModExpr) Accumulate(row int, input *storage.RecordBatch) {
	mod.Left.Accumulate(row, input)
	mod.Right.Accumulate(row, input)
}

func (mod ModExpr) AccumulateValue() []byte {
	leftAccumulateValue := mod.Left.AccumulateValue()
	rightAccumulateValue := mod.Right.AccumulateValue()
	return storage.Mod(leftAccumulateValue, mod.Left.toField().TP, rightAccumulateValue,
		mod.Right.toField().TP)
}

func (mod ModExpr) Clone(cloneAccumulator bool) Expr {
	return ModExpr{
		Left:  mod.Left.Clone(cloneAccumulator),
		Right: mod.Right.Clone(cloneAccumulator),
		Name:  mod.Name,
	}
}

func (mod ModExpr) HasGroupFunc() bool {
	return mod.Left.HasGroupFunc() || mod.Right.HasGroupFunc()
}

func (mod ModExpr) Compute() ([]byte, error) {
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

type EqualExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (equal EqualExpr) toField() storage.Field {
	leftInputField := equal.Left.toField()
	rightInputField := equal.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.EqualOpType)
	f := storage.Field{Name: equal.String(), TP: tp}
	return f
}

func (equal EqualExpr) String() string {
	return fmt.Sprintf("%s = %s", equal.Left, equal.Right)
}

func (equal EqualExpr) TypeCheck() error {
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

func (equal EqualExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := equal.Left.Evaluate(input)
	rightColumnVector := equal.Right.Evaluate(input)
	return leftColumnVector.Equal(rightColumnVector, equal.String())
}

func (equal EqualExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := equal.Left.EvaluateRow(row, input)
	val2 := equal.Right.EvaluateRow(row, input)
	return storage.Equal(val1, equal.Left.toField().TP, val2, equal.Right.toField().TP)
}

func (equal EqualExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (equal EqualExpr) Accumulate(row int, input *storage.RecordBatch) {
	equal.Left.Accumulate(row, input)
	equal.Right.Accumulate(row, input)
}

func (equal EqualExpr) AccumulateValue() []byte {
	leftAccumulateValue := equal.Left.AccumulateValue()
	rightAccumulateValue := equal.Right.AccumulateValue()
	return storage.Equal(leftAccumulateValue, equal.Left.toField().TP, rightAccumulateValue,
		equal.Right.toField().TP)
}

func (equal EqualExpr) Clone(cloneAccumulator bool) Expr {
	return EqualExpr{
		Left:  equal.Left.Clone(cloneAccumulator),
		Right: equal.Right.Clone(cloneAccumulator),
		Name:  equal.Name,
	}
}

func (equal EqualExpr) HasGroupFunc() bool {
	return equal.Left.HasGroupFunc() || equal.Right.HasGroupFunc()
}

func (equal EqualExpr) Compute() ([]byte, error) {
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

type IsExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (is IsExpr) toField() storage.Field {
	leftInputField := is.Left.toField()
	rightInputField := is.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.IsOpType)
	f := storage.Field{Name: is.String(), TP: tp}
	return f
}

func (is IsExpr) String() string {
	return fmt.Sprintf("%s is %s", is.Left, is.Right)
}
func (is IsExpr) TypeCheck() error {
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

func (is IsExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := is.Left.Evaluate(input)
	rightColumnVector := is.Right.Evaluate(input)
	return leftColumnVector.Is(rightColumnVector, is.String())
}

func (is IsExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := is.Left.EvaluateRow(row, input)
	val2 := is.Right.EvaluateRow(row, input)
	return storage.Is(val1, is.Left.toField().TP, val2, is.Right.toField().TP)
}

func (is IsExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (is IsExpr) Accumulate(row int, input *storage.RecordBatch) {
	is.Left.Accumulate(row, input)
	is.Right.Accumulate(row, input)
}

func (is IsExpr) AccumulateValue() []byte {
	leftAccumulateValue := is.Left.AccumulateValue()
	rightAccumulateValue := is.Right.AccumulateValue()
	return storage.Is(leftAccumulateValue, is.Left.toField().TP, rightAccumulateValue,
		is.Right.toField().TP)
}

func (is IsExpr) Clone(cloneAccumulator bool) Expr {
	return IsExpr{
		Left:  is.Left.Clone(cloneAccumulator),
		Right: is.Right.Clone(cloneAccumulator),
		Name:  is.Name,
	}
}

func (is IsExpr) HasGroupFunc() bool {
	return is.Left.HasGroupFunc() || is.Right.HasGroupFunc()
}

func (is IsExpr) Compute() ([]byte, error) {
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

type NotEqualExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (notEqual NotEqualExpr) toField() storage.Field {
	leftInputField := notEqual.Left.toField()
	rightInputField := notEqual.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.NotEqualOpType)
	f := storage.Field{Name: notEqual.String(), TP: tp}
	return f
}

func (notEqual NotEqualExpr) String() string {
	return fmt.Sprintf("%s != %s", notEqual.Left, notEqual.Right)
}
func (notEqual NotEqualExpr) TypeCheck() error {
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

func (notEqual NotEqualExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (notEqual NotEqualExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := notEqual.Left.Evaluate(input)
	rightColumnVector := notEqual.Right.Evaluate(input)
	return leftColumnVector.NotEqual(rightColumnVector, notEqual.String())
}

func (notEqual NotEqualExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := notEqual.Left.EvaluateRow(row, input)
	val2 := notEqual.Right.EvaluateRow(row, input)
	return storage.NotEqual(val1, notEqual.Left.toField().TP, val2, notEqual.Right.toField().TP)
}

func (notEqual NotEqualExpr) Accumulate(row int, input *storage.RecordBatch) {
	notEqual.Left.Accumulate(row, input)
	notEqual.Right.Accumulate(row, input)
}

func (notEqual NotEqualExpr) AccumulateValue() []byte {
	leftAccumulateValue := notEqual.Left.AccumulateValue()
	rightAccumulateValue := notEqual.Right.AccumulateValue()
	return storage.NotEqual(leftAccumulateValue, notEqual.Left.toField().TP, rightAccumulateValue,
		notEqual.Right.toField().TP)
}

func (notEqual NotEqualExpr) Clone(cloneAccumulator bool) Expr {
	return NotEqualExpr{
		Left:  notEqual.Left.Clone(cloneAccumulator),
		Right: notEqual.Right.Clone(cloneAccumulator),
		Name:  notEqual.Name,
	}
}

func (notEqual NotEqualExpr) HasGroupFunc() bool {
	return notEqual.Left.HasGroupFunc() || notEqual.Right.HasGroupFunc()
}

func (notEqual NotEqualExpr) Compute() ([]byte, error) {
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

type GreatExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (great GreatExpr) toField() storage.Field {
	leftInputField := great.Left.toField()
	rightInputField := great.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.GreatOpType)
	f := storage.Field{Name: great.String(), TP: tp}
	return f
}

func (great GreatExpr) String() string {
	return fmt.Sprintf("%s > %s", great.Left, great.Right)
}

func (great GreatExpr) TypeCheck() error {
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

func (great GreatExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := great.Left.Evaluate(input)
	rightColumnVector := great.Right.Evaluate(input)
	return leftColumnVector.Great(rightColumnVector, great.String())
}

func (great GreatExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := great.Left.EvaluateRow(row, input)
	val2 := great.Right.EvaluateRow(row, input)
	return storage.Great(val1, great.Left.toField().TP, val2, great.Right.toField().TP)
}

func (great GreatExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (great GreatExpr) Accumulate(row int, input *storage.RecordBatch) {
	great.Left.Accumulate(row, input)
	great.Right.Accumulate(row, input)
}

func (great GreatExpr) AccumulateValue() []byte {
	leftAccumulateValue := great.Left.AccumulateValue()
	rightAccumulateValue := great.Right.AccumulateValue()
	return storage.Great(leftAccumulateValue, great.Left.toField().TP, rightAccumulateValue,
		great.Right.toField().TP)
}

func (great GreatExpr) Clone(cloneAccumulator bool) Expr {
	return GreatExpr{
		Left:  great.Left.Clone(cloneAccumulator),
		Right: great.Right.Clone(cloneAccumulator),
		Name:  great.Name,
	}
}

func (great GreatExpr) HasGroupFunc() bool {
	return great.Left.HasGroupFunc() || great.Right.HasGroupFunc()
}

func (great GreatExpr) Compute() ([]byte, error) {
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

type GreatEqualExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (greatEqual GreatEqualExpr) toField() storage.Field {
	leftInputField := greatEqual.Left.toField()
	rightInputField := greatEqual.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.GreatEqualOpType)
	f := storage.Field{Name: greatEqual.String(), TP: tp}
	return f
}

func (greatEqual GreatEqualExpr) String() string {
	return fmt.Sprintf("%s >= %s", greatEqual.Left, greatEqual.Right)
}
func (greatEqual GreatEqualExpr) TypeCheck() error {
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

func (greatEqual GreatEqualExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := greatEqual.Left.Evaluate(input)
	rightColumnVector := greatEqual.Right.Evaluate(input)
	return leftColumnVector.GreatEqual(rightColumnVector, greatEqual.String())
}

func (greatEqual GreatEqualExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := greatEqual.Left.EvaluateRow(row, input)
	val2 := greatEqual.Right.EvaluateRow(row, input)
	return storage.GreatEqual(val1, greatEqual.Left.toField().TP, val2, greatEqual.Right.toField().TP)
}

func (greatEqual GreatEqualExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (greatEqual GreatEqualExpr) Accumulate(row int, input *storage.RecordBatch) {
	greatEqual.Left.Accumulate(row, input)
	greatEqual.Right.Accumulate(row, input)
}

func (greatEqual GreatEqualExpr) AccumulateValue() []byte {
	leftAccumulateValue := greatEqual.Left.AccumulateValue()
	rightAccumulateValue := greatEqual.Right.AccumulateValue()
	return storage.GreatEqual(leftAccumulateValue, greatEqual.Left.toField().TP, rightAccumulateValue,
		greatEqual.Right.toField().TP)
}

func (greatEqual GreatEqualExpr) Clone(cloneAccumulator bool) Expr {
	return GreatEqualExpr{
		Left:  greatEqual.Left.Clone(cloneAccumulator),
		Right: greatEqual.Right.Clone(cloneAccumulator),
		Name:  greatEqual.Name,
	}
}

func (greatEqual GreatEqualExpr) HasGroupFunc() bool {
	return greatEqual.Left.HasGroupFunc() || greatEqual.Right.HasGroupFunc()
}

func (greatEqual GreatEqualExpr) Compute() ([]byte, error) {
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

type LessExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (less LessExpr) toField() storage.Field {
	leftInputField := less.Left.toField()
	rightInputField := less.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.LessOpType)
	f := storage.Field{Name: less.String(), TP: tp}
	return f
}

func (less LessExpr) String() string {
	return fmt.Sprintf("%s < %s", less.Left, less.Right)
}

func (less LessExpr) TypeCheck() error {
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

func (less LessExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := less.Left.Evaluate(input)
	rightColumnVector := less.Right.Evaluate(input)
	return leftColumnVector.Less(rightColumnVector, less.String())
}

func (less LessExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := less.Left.EvaluateRow(row, input)
	val2 := less.Right.EvaluateRow(row, input)
	return storage.Less(val1, less.Left.toField().TP, val2, less.Right.toField().TP)
}

func (less LessExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (less LessExpr) Accumulate(row int, input *storage.RecordBatch) {
	less.Left.Accumulate(row, input)
	less.Right.Accumulate(row, input)
}

func (less LessExpr) AccumulateValue() []byte {
	leftAccumulateValue := less.Left.AccumulateValue()
	rightAccumulateValue := less.Right.AccumulateValue()
	return storage.Less(leftAccumulateValue, less.Left.toField().TP, rightAccumulateValue,
		less.Right.toField().TP)
}

func (less LessExpr) Clone(cloneAccumulator bool) Expr {
	return LessExpr{
		Left:  less.Left.Clone(cloneAccumulator),
		Right: less.Right.Clone(cloneAccumulator),
		Name:  less.Name,
	}
}

func (less LessExpr) HasGroupFunc() bool {
	return less.Left.HasGroupFunc() || less.Right.HasGroupFunc()
}

func (less LessExpr) Compute() ([]byte, error) {
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

type LessEqualExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (lessEqual LessEqualExpr) toField() storage.Field {
	leftInputField := lessEqual.Left.toField()
	rightInputField := lessEqual.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.LessEqualOpType)
	f := storage.Field{Name: lessEqual.String(), TP: tp}
	return f
}
func (lessEqual LessEqualExpr) String() string {
	return fmt.Sprintf("%s <= %s", lessEqual.Left, lessEqual.Right)
}
func (lessEqual LessEqualExpr) TypeCheck() error {
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

func (lessEqual LessEqualExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := lessEqual.Left.Evaluate(input)
	rightColumnVector := lessEqual.Right.Evaluate(input)
	return leftColumnVector.LessEqual(rightColumnVector, lessEqual.String())
}

func (lessEqual LessEqualExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := lessEqual.Left.EvaluateRow(row, input)
	val2 := lessEqual.Right.EvaluateRow(row, input)
	return storage.LessEqual(val1, lessEqual.Left.toField().TP, val2, lessEqual.Right.toField().TP)
}

func (lessEqual LessEqualExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (lessEqual LessEqualExpr) Accumulate(row int, input *storage.RecordBatch) {
	lessEqual.Left.Accumulate(row, input)
	lessEqual.Right.Accumulate(row, input)
}

func (lessEqual LessEqualExpr) AccumulateValue() []byte {
	leftAccumulateValue := lessEqual.Left.AccumulateValue()
	rightAccumulateValue := lessEqual.Right.AccumulateValue()
	return storage.LessEqual(leftAccumulateValue, lessEqual.Left.toField().TP, rightAccumulateValue,
		lessEqual.Right.toField().TP)
}

func (lessEqual LessEqualExpr) Clone(cloneAccumulator bool) Expr {
	return LessEqualExpr{
		Left:  lessEqual.Left.Clone(cloneAccumulator),
		Right: lessEqual.Right.Clone(cloneAccumulator),
		Name:  lessEqual.Name,
	}
}

func (lessEqual LessEqualExpr) HasGroupFunc() bool {
	return lessEqual.Left.HasGroupFunc() || lessEqual.Right.HasGroupFunc()
}

func (lessEqual LessEqualExpr) Compute() ([]byte, error) {
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

type AndExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (and AndExpr) toField() storage.Field {
	leftInputField := and.Left.toField()
	rightInputField := and.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.AndOpType)
	f := storage.Field{Name: and.String(), TP: tp}
	return f
}

func (and AndExpr) String() string {
	return fmt.Sprintf("%s and %s", and.Left, and.Right)
}

func (and AndExpr) TypeCheck() error {
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

func (and AndExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := and.Left.Evaluate(input)
	rightColumnVector := and.Right.Evaluate(input)
	return leftColumnVector.And(rightColumnVector, and.String())
}

func (and AndExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := and.Left.EvaluateRow(row, input)
	val2 := and.Right.EvaluateRow(row, input)
	return storage.And(val1, val2)
}

func (and AndExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (and AndExpr) Accumulate(row int, input *storage.RecordBatch) {
	and.Left.Accumulate(row, input)
	and.Right.Accumulate(row, input)
}

func (and AndExpr) AccumulateValue() []byte {
	leftAccumulateValue := and.Left.AccumulateValue()
	rightAccumulateValue := and.Right.AccumulateValue()
	return storage.And(leftAccumulateValue, rightAccumulateValue)
}

func (and AndExpr) Clone(cloneAccumulator bool) Expr {
	return AndExpr{
		Left:  and.Left.Clone(cloneAccumulator),
		Right: and.Right.Clone(cloneAccumulator),
		Name:  and.Name,
	}
}

func (and AndExpr) Compute() ([]byte, error) {
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

func (and AndExpr) HasGroupFunc() bool {
	return and.Left.HasGroupFunc() || and.Right.HasGroupFunc()
}

type OrExpr struct {
	Left  Expr
	Right Expr
	Name  string
}

func (or OrExpr) toField() storage.Field {
	leftInputField := or.Left.toField()
	rightInputField := or.Right.toField()
	tp := leftInputField.InferenceType(rightInputField, storage.OrOpType)
	f := storage.Field{Name: or.String(), TP: tp}
	return f
}

func (or OrExpr) String() string {
	return fmt.Sprintf("%s or %s", or.Left, or.Right)
}
func (or OrExpr) TypeCheck() error {
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

func (or OrExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	leftColumnVector := or.Left.Evaluate(input)
	rightColumnVector := or.Right.Evaluate(input)
	return leftColumnVector.Or(rightColumnVector, or.String())
}

func (or OrExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	val1 := or.Left.EvaluateRow(row, input)
	val2 := or.Right.EvaluateRow(row, input)
	return storage.Or(val1, val2)
}

func (or OrExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (or OrExpr) Accumulate(row int, input *storage.RecordBatch) {
	or.Left.Accumulate(row, input)
	or.Right.Accumulate(row, input)
}

func (or OrExpr) AccumulateValue() []byte {
	leftAccumulateValue := or.Left.AccumulateValue()
	rightAccumulateValue := or.Right.AccumulateValue()
	return storage.Or(leftAccumulateValue, rightAccumulateValue)
}

func (or OrExpr) Clone(cloneAccumulator bool) Expr {
	return OrExpr{
		Left:  or.Left.Clone(cloneAccumulator),
		Right: or.Right.Clone(cloneAccumulator),
		Name:  or.Name,
	}
}

func (or OrExpr) HasGroupFunc() bool {
	return or.Left.HasGroupFunc() || or.Right.HasGroupFunc()
}

func (or OrExpr) Compute() ([]byte, error) {
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

type OrderByExpr struct {
	Expr []Expr
	Asc  []bool
}

func (orderBy OrderByExpr) toField(input Plan) storage.Field {
	return storage.Field{Name: "order", TP: storage.DefaultFieldTpMap[storage.Int]}
}

func (orderBy OrderByExpr) String() string {
	buf := bytes.Buffer{}
	buf.WriteString("orderBy(")
	for i, expr := range orderBy.Expr {
		asc := "Asc"
		if !orderBy.Asc[i] {
			asc = "desc"
		}
		buf.WriteString(fmt.Sprintf("[%s, %s]", expr, asc))
		if i != len(orderBy.Expr)-1 {
			buf.WriteString(",")
		}
	}
	return buf.String()
}

func (orderBy OrderByExpr) TypeCheck() error {
	// only numerical or string type can be orderBy, aka comparable type.
	for _, expr := range orderBy.Expr {
		err := expr.TypeCheck()
		if err != nil {
			return err
		}
		// Todo: do we need this?
		//if !Expr.toField(input).IsComparable() {
		//	return errors.New("order by cannot be applied to")
		//}
	}
	return nil
}

func (orderBy OrderByExpr) AggrTypeCheck(groupByExpr []Expr) error {
	found := false
	for _, orderByExpr := range orderBy.Expr {
		if orderByExpr.AggrTypeCheck(groupByExpr) != nil {
			found = true
		}
	}
	if !found {
		return nil
	}
	for _, orderByExpr := range orderBy.Expr {
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
// so the orderBy Executor can change the input to:
// 09, 2
// 10, 1
// 11, 3
func (orderBy OrderByExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	ret := &storage.ColumnVector{Field: storage.Field{Name: "order", TP: storage.DefaultFieldTpMap[storage.Int]}}
	for i := 0; i < input.RowCount(); i++ {
		val := storage.EncodeInt(int64(i))
		ret.Append(val)
	}
	sortedVector := make([]*storage.ColumnVector, len(orderBy.Expr))
	asc := make([]bool, len(orderBy.Expr))
	for i, expr := range orderBy.Expr {
		columnVector := expr.Evaluate(input)
		sortedVector[i] = columnVector
		asc[i] = orderBy.Asc[i]
	}
	return ret.Sort(sortedVector, asc)
}

func (orderBy OrderByExpr) Compute() []byte {
	panic("unsupported method")
}

// For sql, there are some aggregation function and non aggregation function. We put all non aggregation function
// here.
type FuncCallExpr struct {
	FuncName string
	Params   []Expr
	Name     string
	Fn       FuncInterface `json:"-"`
}

func (call *FuncCallExpr) toField() storage.Field {
	return storage.Field{Name: call.Fn.String(), TP: call.Fn.ReturnType()}
}

func (call *FuncCallExpr) String() string {
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

func (call *FuncCallExpr) TypeCheck() error {
	f := call.Fn
	if f == nil {
		return errors.New("no such func")
	}
	paramFields := make([]storage.Field, len(call.Params))
	for i, param := range call.Params {
		err := param.TypeCheck()
		if err != nil {
			return err
		}
		paramFields[i] = param.toField()
	}
	err := f.TypeCheck()
	if err != nil {
		return err
	}
	// Now we check valid group by.
	// It will fail in select sum(sum(id)) from test;
	if !f.IsAggrFunc() {
		return nil
	}
	for _, param := range call.Params {
		if param.HasGroupFunc() {
			return errors.New("invalid use of group function")
		}
	}
	return nil
}

func (call *FuncCallExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	f := call.Fn
	columnVectors := make([]*storage.ColumnVector, len(call.Params))
	for i, param := range call.Params {
		columnVectors[i] = param.Evaluate(input)
	}
	ret := &storage.ColumnVector{Field: storage.Field{Name: call.Name, TP: f.ReturnType()}}
	for i := 0; i < columnVectors[0].Size(); i++ {
		params := make([][]byte, len(columnVectors))
		for j, columnVector := range columnVectors {
			params[j] = columnVector.RawValue(i)
		}
		oneRecord := f.F()(params)
		ret.Append(oneRecord)
	}
	return ret
}

func (call *FuncCallExpr) IsAggrFunc() bool {
	return call.Fn.IsAggrFunc()
}

func (call *FuncCallExpr) AggrTypeCheck(groupByExpr []Expr) error {
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

func (call *FuncCallExpr) Accumulate(row int, input *storage.RecordBatch) {
	call.Fn.Accumulate(row, input)
}

func (call *FuncCallExpr) AccumulateValue() []byte {
	return call.Fn.AccumulateValue()
}

func (call *FuncCallExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	params := make([][]byte, len(call.Params))
	for i := 0; i < len(call.Params); i++ {
		params[i] = call.Params[i].EvaluateRow(row, input)
	}
	return call.Fn.F()(params)
}

func (call *FuncCallExpr) Clone(cloneAccumulate bool) Expr {
	ret := &FuncCallExpr{
		FuncName: call.FuncName,
		Params:   make([]Expr, len(call.Params)),
		Name:     call.Name,
	}
	for i, expr := range call.Params {
		ret.Params[i] = expr.Clone(cloneAccumulate)
	}
	ret.Fn = getFunc(call.FuncName, call.Params)
	return ret
}

func (call *FuncCallExpr) Compute() ([]byte, error) {
	return nil, errors.New("unsupported method")
}

func (call *FuncCallExpr) HasGroupFunc() bool {
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

func MakeFuncCallExpr(name string, params []Expr) *FuncCallExpr {
	return &FuncCallExpr{
		FuncName: name,
		Name:     name,
		Fn:       getFunc(name, params),
		Params:   params,
	}
}

type AsExpr struct {
	Expr  Expr
	Alias string
}

func (as AsExpr) toField() storage.Field {
	exprField := as.Expr.toField()
	f := storage.Field{Name: exprField.Name, Alias: as.Alias, TP: exprField.TP}
	return f
}

func (as AsExpr) String() string {
	return as.Alias
}
func (as AsExpr) TypeCheck() error {
	return as.Expr.TypeCheck()
}

func (as AsExpr) Evaluate(input *storage.RecordBatch) *storage.ColumnVector {
	old := as.Expr.Evaluate(input)
	field := as.toField()
	ret := &storage.ColumnVector{Field: field}
	ret.Appends(old)
	return ret
}

func (as AsExpr) AggrTypeCheck(groupByExpr []Expr) error {
	return as.Expr.AggrTypeCheck(groupByExpr)
}

func (as AsExpr) Clone(needAccumulator bool) Expr {
	return AsExpr{
		Expr:  as.Expr.Clone(needAccumulator),
		Alias: as.Alias,
	}
}

func (as AsExpr) EvaluateRow(row int, input *storage.RecordBatch) []byte {
	return as.Expr.EvaluateRow(row, input)
}

func (as AsExpr) Accumulate(row int, input *storage.RecordBatch) {
	as.Expr.Accumulate(row, input)
}

func (as AsExpr) AccumulateValue() []byte {
	return as.Expr.AccumulateValue()
}
func (as AsExpr) HasGroupFunc() bool {
	return as.Expr.HasGroupFunc()
}

func (as AsExpr) Compute() ([]byte, error) {
	return as.Expr.Compute()
}
