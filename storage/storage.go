package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

type Storage struct {
	Dbs map[string]*DbInfo
}

func (storage Storage) HasSchema(schema string) bool {
	_, ok := storage.Dbs[schema]
	return ok
}

func (storage Storage) GetDbInfo(schema string) *DbInfo {
	return storage.Dbs[schema]
}

func (storage Storage) CreateSchema(name, charset, collate string) {
	schema := &DbInfo{Name: name, Charset: charset, Collate: collate}
	storage.Dbs[schema.Name] = schema
}

func (storage Storage) RemoveSchema(schema string) {
	delete(storage.Dbs, schema)
}

func (storage Storage) HasTable(schema, table string) bool {
	if !storage.HasSchema(schema) {
		return false
	}
	db := storage.GetDbInfo(schema)
	return db.HasTable(table)
}

var storage = Storage{Dbs: map[string]*DbInfo{}}

func GetStorage() Storage {
	return storage
}

type DbInfo struct {
	Name    string
	Charset string
	Collate string
	Tables  map[string]*TableInfo
}

func (dbs *DbInfo) HasTable(table string) bool {
	_, ok := dbs.Tables[table]
	return ok
}

func (dbs *DbInfo) GetTable(table string) *TableInfo {
	return dbs.Tables[table]
}

func (dbs *DbInfo) AddTable(table *TableInfo) {
	dbs.Tables[table.Schema.Tables[0].TableName] = table
}

func (dbs *DbInfo) RemoveTable(tableName string) {
	delete(dbs.Tables, tableName)
}

type TableInfo struct {
	Schema  Schema
	Charset string
	Collate string
	Engine  string
	Datas   []ColumnVector
}

// This Is for join, a schema might have multiple sub table schemas.
type Schema struct {
	Tables []SingleTableSchema
	// Name   string // Because this can support join tables. do we need name?
}

// A table format looks like this.
// | rowIndex | cols ... | DefaultPrimaryKey (if cols doesn't have primary key column |
// the rowIndex column has no content by default. But when the fetch data is called.
// We will feed the row index value to the rowIndex column.

// A SingleTableSchema is a list of Fields representing a temporal table format.
// It can has multiple columns, each column has a DatabaseName, TableRef, ColumnName to allow
// multiple columns coexist with same columnName but are from different database.
type SingleTableSchema struct {
	Columns    []Field
	TableName  string
	SchemaName string
}

func (schema SingleTableSchema) AppendColumn(field Field) {
	schema.Columns = append(schema.Columns, field)
}

// FetchData returns the data starting at row index `rowIndex` And the batchSize Is batchSize.
func (table *TableInfo) FetchData(rowIndex, batchSize int) *RecordBatch {
	// The records in the last column is the row index if needRowIndex is true.
	ret := &RecordBatch{
		Fields:  table.Schema.Tables[0].Columns,
		Records: make([]ColumnVector, len(table.Schema.Tables[0].Columns)),
	}
	ret.Fields = append(ret.Fields, RowIndexField)
	ret.Records = append(ret.Records, ColumnVector{Field: RowIndexField})
	if len(table.Datas) == 0 {
		return ret
	}
	for i := rowIndex; i < batchSize && i < table.Datas[0].Size(); i++ {
		for j, col := range table.Datas {
			if j == 0 {
				// The first row is the row index.
				ret.Records[0].Append(EncodeInt(int64(rowIndex)))
				continue
			}
			ret.Records[j].Append(col.Values[i])
		}
	}
	return ret
}

func (table *TableInfo) GetColumnInfo(column string) *Field {
	for _, col := range table.Datas {
		if col.Field.Name == column {
			return &col.Field
		}
	}
	return nil
}

func (table *TableInfo) HasColumn(column string) bool {
	for _, col := range table.Datas {
		if col.Field.Name == column {
			return true
		}
	}
	return false
}

func DefaultPrimaryKeyColumn() Field {
	return Field{Name: "_id", TP: Int, PrimaryKey: true, AutoIncrement: true, AllowNull: false}
}

// update tableInfo col to new value `value` at row index row.
func (table *TableInfo) UpdateData(colName string, row int, value []byte) {
	for _, col := range table.Datas {
		if col.Field.Name == colName {
			// update this column.
			col.Values[row] = value
		}
	}
}

func (table *TableInfo) DeleteRow(row int) {
	for i := 0; i < len(table.Datas); i++ {
		table.Datas[i].Values = append(table.Datas[i].Values[:i], table.Datas[i].Values[i+1:]...)
	}
}

func (table TableInfo) Truncate() {
	for i := 0; i < len(table.Datas); i++ {
		table.Datas[i].Values = nil
	}
}

func (table TableInfo) InsertData(cols []string, values [][]byte) {
	for i, col := range cols {
		for _, tableCol := range table.Datas {
			if tableCol.Field.Name == col {
				tableCol.Append(values[i])
			}
		}
	}
}

func (schema Schema) HasSubTable(tableName string) bool {
	for _, table := range schema.Tables {
		if table.TableName == tableName {
			return true
		}
	}
	return false
}

func (schema Schema) GetSubTableFromColumn(schemaName, tableName, col string) (SingleTableSchema, error) {
	for _, table := range schema.Tables {
		// Schema can be empty
		if schemaName == "" && table.TableName == tableName && schema.TableHasColumn(table.Columns, col) {
			return table, nil
		}
		if schemaName == "" && tableName == "" && schema.TableHasColumn(table.Columns, col) {
			return table, nil
		}
		if schemaName != "" && (schemaName == table.SchemaName) && table.TableName == tableName &&
			schema.TableHasColumn(table.Columns, col) {
			return table, nil
		}
	}
	return SingleTableSchema{}, errors.New("cannot find such table")
}

// HasColumn returns whether this schema has such schema, table And column.
// schemaName, tableName can be empty, then it will iterate all db schema to find such column.
func (schema Schema) HasColumn(schemaName, tableName, columnName string) bool {
	for _, table := range schema.Tables {
		// Schema can be empty
		if schemaName == "" && table.TableName == tableName && schema.TableHasColumn(table.Columns, columnName) {
			return true
		}
		if schemaName == "" && tableName == "" && schema.TableHasColumn(table.Columns, columnName) {
			return true
		}
		if schemaName != "" && (schemaName == table.SchemaName) && table.TableName == tableName &&
			schema.TableHasColumn(table.Columns, columnName) {
			return true
		}
	}
	return false
}

func (schema Schema) TableHasColumn(fields []Field, column string) bool {
	for _, f := range fields {
		if f.Name == column {
			return true
		}
	}
	return false
}

func (schema Schema) HasAmbiguousColumn(schemaName, tableName, columnName string) bool {
	if schemaName != "" && tableName != "" {
		return false
	}
	times := 0
	for _, table := range schema.Tables {
		if tableName == "" && schema.TableHasColumn(table.Columns, columnName) {
			times++
		}
		if tableName != "" && tableName == table.TableName && schema.TableHasColumn(table.Columns, columnName) {
			times++
		}
	}
	return times > 1
}

var emptyField = Field{}

func (schema Schema) GetField(columnName string) Field {
	for _, table := range schema.Tables {
		for _, field := range table.Columns {
			if field.Name == columnName {
				return field
			}
		}
	}
	return emptyField
}

func (schema Schema) Merge(right Schema) (Schema, error) {
	ret := Schema{} // Are we safe here.
	ret.Tables = append(ret.Tables, schema.Tables...)
	ret.Tables = append(ret.Tables, right.Tables...)
	return ret, nil
}

type RecordBatch struct {
	Fields  []Field        `json:"fields"`
	Records []ColumnVector `json:"records"`
}

func (recordBatch *RecordBatch) RowCount() int {
	for _, r := range recordBatch.Records {
		return r.Size()
	}
	return 0
}

var emptyColumnVector = ColumnVector{}

func (recordBatch *RecordBatch) GetColumnValue(colName string) ColumnVector {
	for _, col := range recordBatch.Records {
		if col.Field.Name == colName {
			return col
		}
	}
	// Todo: do we need to change it to pointer
	return emptyColumnVector
}

func (recordBatch *RecordBatch) ColumnCount() int {
	return len(recordBatch.Records)
}

// recordBatch join another.
func (recordBatch *RecordBatch) Join(another *RecordBatch) *RecordBatch {
	ret := &RecordBatch{
		Fields:  make([]Field, len(recordBatch.Fields)+len(another.Fields)),
		Records: make([]ColumnVector, len(recordBatch.Records)+len(another.Records)),
	}
	// set Field first.
	for i, f := range recordBatch.Fields {
		ret.Fields[i] = f
	}
	j := len(recordBatch.Fields)
	for i, f := range another.Fields {
		ret.Fields[j+i] = f
	}
	// set column vector.
	for i, col := range recordBatch.Records {
		ret.Records[i] = col
	}
	j = len(recordBatch.Fields)
	for i, col := range another.Records {
		ret.Records[i+j] = col
	}
	return ret
}

// Append new to recordBatch, they are in the same layout.
func (recordBatch *RecordBatch) Append(new *RecordBatch) {
	for i, col := range new.Records {
		recordBatch.Records[i].Appends(col.Values)
	}
}

// columnVector represents the order of recordBatch. It's has just one row.
// whose field Is Field{Name: "order", TP: storage.Int}.
func (recordBatch *RecordBatch) OrderBy(columnVector ColumnVector) {
	temp := &RecordBatch{Fields: recordBatch.Fields, Records: make([]ColumnVector, len(recordBatch.Records))}
	for i, col := range recordBatch.Records {
		temp.Records[i].Field = temp.Fields[i]
		temp.Records[i].Values = make([][]byte, len(col.Values))
	}
	// Reorder
	for j := 0; j < columnVector.Size(); j++ {
		// Move j -> newIndex
		newIndex := columnVector.Int(j)
		for i, col := range recordBatch.Records {
			temp.Records[i].Values[newIndex] = col.Values[j]
		}
	}
	recordBatch.Copy(temp, 0, 0, temp.RowCount())
}

// Set the i-th column values in recordBatch by using columnVector.
func (recordBatch *RecordBatch) SetColumnValue(col int, columnVector ColumnVector) {
	recordBatch.Records[col] = columnVector
}

// Append row i of record to recordBatch.
func (recordBatch *RecordBatch) AppendRecord(record *RecordBatch, row int) {
	for col := 0; col < recordBatch.ColumnCount(); col++ {
		recordBatch.Records[col].Append(record.Records[col].Values[row])
	}
}

// Copy src[srcFrom:srcFrom + size] to recordBatch[descFrom:], recordBatch must have enough memory to save all data.
func (recordBatch *RecordBatch) Copy(src *RecordBatch, srcFrom, descFrom, size int) {
	for i := srcFrom; i < srcFrom+size && i < src.RowCount(); i++ {
		// Copy one row.
		for j := 0; j < src.ColumnCount(); j++ {
			recordBatch.Records[j].Values[i] = src.Records[j].Values[i]
		}
	}
}

// selectedRows Is a bool column which represent each row in recordBatch Is selected Or not.
func (recordBatch *RecordBatch) Filter(selectedRows ColumnVector) *RecordBatch {
	ret := MakeEmptyRecordBatchFrom(recordBatch)
	// now we copy the selected rows.
	for i := 0; i < recordBatch.RowCount(); i++ {
		if !selectedRows.Bool(i) {
			continue
		}
		for j := 0; j < recordBatch.ColumnCount(); j++ {
			ret.Records[j].Append(recordBatch.Records[j].Values[i])
		}
	}
	return ret
}

// Return data[startIndex: startIndex + size - 1]
func (recordBatch *RecordBatch) Slice(startIndex, size int) *RecordBatch {
	ret := MakeEmptyRecordBatchFrom(recordBatch)
	for i := startIndex; i < startIndex+size && i < ret.RowCount(); i++ {
		// Copy one row.
		for j := 0; j < recordBatch.ColumnCount(); j++ {
			ret.Records[j].Values[i] = recordBatch.Records[j].Values[i]
		}
	}
	return ret
}

func MakeEmptyRecordBatchFrom(src *RecordBatch) *RecordBatch {
	ret := &RecordBatch{
		Fields:  make([]Field, src.ColumnCount()),
		Records: make([]ColumnVector, src.ColumnCount()),
	}
	// copy field And column vector field first.
	for i, f := range src.Fields {
		ret.Fields[i] = f
		ret.Records[i].Field = f
	}
	return ret
}

// Encode row key.
func (recordBatch *RecordBatch) RowKey(row int) (key []byte) {
	if recordBatch.RowCount() >= row {
		return
	}
	for i := 0; i < recordBatch.ColumnCount(); i++ {
		key = append(key, ',')
		key = append(key, recordBatch.Records[i].Values[row]...)
	}
	return
}

// Return the rowIndex in the row-th data.
func (recordBatch *RecordBatch) RowIndex(tableName string, row int) (int, error) {
	for i := 0; i < recordBatch.ColumnCount(); i++ {
		if recordBatch.Fields[i].TableName == tableName {
			return int(DecodeInt(recordBatch.Records[i].Values[row])), nil
		}
	}
	return 0, errors.New("unable found such table")
}

// For type check.
type Field struct {
	TP            FieldTP
	Name          string
	TableName     string
	SchemaName    string
	DefaultValue  []byte
	AllowNull     bool
	AutoIncrement bool
	PrimaryKey    bool
}

func (f Field) IsString() bool {
	return f.TP == Char || f.TP == VarChar || f.TP == Text || f.TP == MediumText || f.TP == DateTime ||
		f.TP == Blob || f.TP == MediumBlob
}

func (f Field) IsNumerical() bool {
	return f.TP == Int || f.TP == Float
}

func (f Field) IsBool() bool {
	return f.TP == Bool
}

func (f Field) IsInteger() bool {
	return f.TP == Int
}

func (f Field) IsFloat() bool {
	return f.TP == Float
}

func (f Field) CanOp(another Field, opType OpType) (err error) {
	switch opType {
	case NegativeOpType:
		if f.IsNumerical() {
			err = errors.New("- cannot apply to non numerical type")
		}
		return nil
	case AddOpType, MinusOpType, MulOpType, DivideOpType:
		if f.IsNumerical() && another.IsNumerical() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s cannot apply to non numerical type", opType))
	case ModOpType:
		if f.IsInteger() && another.IsInteger() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s cannot apply to non integer type", opType))
	case AndOpType, OrOpType:
		if f.IsBool() && another.IsBool() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s cannot apply to non bool type", opType))
	case EqualOpType, NotEqualOpType, IsOpType:
		if f.IsNumerical() && another.IsNumerical() {
			return nil
		}
		if f.IsString() && another.IsString() {
			return nil
		}
		if f.IsBool() && another.IsBool() {
			return nil
		}
		return errors.New(fmt.Sprintf("type doesn't match on %s", opType))
	case LessOpType, LessEqualOpType, GreatEqualOpType, GreatOpType:
		if f.IsNumerical() && another.IsNumerical() {
			return nil
		}
		if f.IsString() && another.IsString() {
			return nil
		}
		if f.IsBool() && another.IsBool() {
			return nil
		}
		return errors.New(fmt.Sprintf("type doesn't match on %s", opType))
	default:
		panic("wrong opType")
	}
}

func (f Field) CanOp2(another FieldTP, opType OpType) (err error) {
	f2 := Field{TP: another}
	return f.CanOp(f2, opType)
}

const rowIndexName = "_rowid"

var RowIndexField = Field{Name: rowIndexName, TP: Int, AllowNull: true, AutoIncrement: false, PrimaryKey: false}

type OpType byte

const (
	AddOpType OpType = iota
	MinusOpType
	MulOpType
	DivideOpType
	AndOpType
	OrOpType
	ModOpType
	GreatOpType
	GreatEqualOpType
	LessOpType
	LessEqualOpType
	EqualOpType
	NotEqualOpType
	IsOpType
	NegativeOpType
)

func (tp OpType) String() string {
	switch tp {
	case AddOpType:
		return "+"
	case MinusOpType:
		return "-"
	case MulOpType:
		return "*"
	case DivideOpType:
		return "/"
	case AndOpType:
		return "&&"
	case OrOpType:
		return "||"
	case ModOpType:
		return "%"
	case GreatOpType:
		return ">"
	case GreatEqualOpType:
		return ">="
	case LessOpType:
		return "<"
	case LessEqualOpType:
		return "<="
	case EqualOpType:
		return "="
	case NotEqualOpType:
		return "!="
	case IsOpType:
		return "Is"
	case NegativeOpType:
		return "-"
	default:
		panic("unknown op")
	}
}

func (tp OpType) Comparator() bool {
	return tp == IsOpType || tp == EqualOpType || tp == NotEqualOpType || tp == GreatOpType ||
		tp == GreatEqualOpType || tp == LessOpType || tp == LessEqualOpType
}

func (tp OpType) Logic() bool {
	return tp == AndOpType || tp == OrOpType
}

var typeOpMap = map[string]FieldTP{
	"int + int":      Int,
	"int + float":    Float,
	"float + int":    Float,
	"float + float":  Float,
	"int - int":      Int,
	"int - float":    Float,
	"float - int":    Float,
	"float - float":  Float,
	"int * int":      Int,
	"int * float":    Float,
	"float * int":    Float,
	"float * float":  Float,
	"int / int":      Int,
	"int / float":    Float,
	"float / int":    Float,
	"float / float":  Float,
	"int % int":      Int,
	"int = int":      Bool,
	"int = float":    Bool,
	"float = int":    Bool,
	"float = float":  Bool,
	"int Is int":     Bool,
	"int Is float":   Bool,
	"float Is int":   Bool,
	"float Is float": Bool,
	"int != int":     Bool,
}

// Return the new type after we apply f op another.
func (f Field) InferenceType(another Field, op OpType) FieldTP {
	if op.Comparator() {
		return Bool
	}
	if op.Logic() {
		return Bool
	}
	key := fmt.Sprintf("%s %s %s", f.TP, another.TP, op)
	return typeOpMap[key]
}

func InferenceType(data []byte) FieldTP {
	if string(data) == "true" || string(data) == "false" {
		return Bool
	}
	if data[0] >= '0' && data[0] <= '9' {
		InferenceNumericalType(data)
	}
	if data[0] == '.' {
		// must a float type.
		return Float
	}
	if data[0] == '\'' || data[0] == '"' {
		return Text
	}
	panic("unknown data type")
}

func InferenceNumericalType(data []byte) FieldTP {
	if bytes.IndexByte(data, '.') == -1 {
		return Int
	}
	return Float
}

// A column of field.
type ColumnVector struct {
	Field  Field
	Values [][]byte
}

func (column ColumnVector) GetField() Field {
	return column.Field
}

func (column ColumnVector) GetTP() FieldTP {
	return column.Field.TP
}

func (column ColumnVector) Size() int {
	return len(column.Values)
}

func (column ColumnVector) RawValue(row int) []byte {
	return column.Values[row]
}

func (column ColumnVector) Negative() ColumnVector {
	// column must be a numeric type
	ret := ColumnVector{Field: column.Field}
	for _, value := range column.Values {
		v := Negative(column.Field.TP, value)
		ret.Values = append(ret.Values, v)
	}
	return ret
}

// Add another And column And return the new column with name `name`
func (column ColumnVector) Add(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{TP: column.Field.InferenceType(another.Field, AddOpType), Name: name},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Add(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

// Minus another And column And return the new column with name `name`
func (column ColumnVector) Minus(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{TP: column.Field.InferenceType(another.Field, MinusOpType), Name: name},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Minus(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Mul(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{TP: column.Field.InferenceType(another.Field, MulOpType), Name: name},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Mul(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Divide(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{TP: column.Field.InferenceType(another.Field, DivideOpType), Name: name},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Divide(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Mod(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{TP: column.Field.InferenceType(another.Field, ModOpType), Name: name},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Mod(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Equal(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(Equal(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Is(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(Is(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) NotEqual(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(NotEqual(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Great(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(Great(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) GreatEqual(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(GreatEqual(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) Less(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(Less(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) LessEqual(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(LessEqual(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column ColumnVector) And(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(And(val1, val2))
	}
	return ret
}

func (column ColumnVector) Or(another ColumnVector, name string) ColumnVector {
	ret := ColumnVector{
		Field:  Field{Name: name, TP: Bool},
		Values: make([][]byte, column.Size()),
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := column.RawValue(i)
		ret.Append(Or(val1, val2))
	}
	return ret
}

// sort column by using order specified by others in order asc.
func (column ColumnVector) Sort(others []ColumnVector, asc []bool) ColumnVector {
	ret := ColumnVector{Field: column.Field, Values: make([][]byte, column.Size())}
	ret.Appends(column.Values)
	sort.Slice(ret.Values, func(i, j int) bool {
		for i := 0; i < len(others); i++ {
			c := compare(ret.Values[i], column.GetTP(), ret.Values[j], column.GetTP())
			if c == 0 {
				continue
			}
			if c < 0 {
				return asc[i]
			}
			if c > 0 {
				return !asc[i]
			}
		}
		return i < j
	})
	return ret
}

// column must be a bool column
func (column ColumnVector) Bool(row int) bool {
	return DecodeBool(column.Values[row])
}

// column must a integer column.
func (column ColumnVector) Int(row int) int64 {
	return DecodeInt(column.Values[row])
}

func (column ColumnVector) Append(value []byte) {
	column.Values = append(column.Values, value)
}

func (column ColumnVector) Appends(values [][]byte) {
	column.Values = append(column.Values, values...)
}

func (column ColumnVector) ToString(row int) string {
	switch column.Field.TP {
	case Text, Char, VarChar, MediumText, Blob, MediumBlob, DateTime:
		// we can compare them by bytes.
		return string(column.Values[row])
	case Bool:
		if column.Bool(row) {
			return "1"
		}
		return "0"
	case Int:
		return strconv.FormatInt(DecodeInt(column.Values[row]), 10)
	case Float:
		v := DecodeFloat(column.Values[row])
		return fmt.Sprintf("%f", v)
	default:
		panic("unknown type")
	}
}

type FieldTP string

const (
	Bool       FieldTP = "bool"
	Int        FieldTP = "int"
	Float      FieldTP = "float"
	Char       FieldTP = "char"
	VarChar    FieldTP = "varchar"
	DateTime   FieldTP = "datetime"
	Blob       FieldTP = "blob"
	MediumBlob FieldTP = "mediumBlob"
	Text       FieldTP = "text"
	MediumText FieldTP = "mediumText"
	// Todo: We might support big int later.
	// BigInt     FieldTP = "bigint"
)
