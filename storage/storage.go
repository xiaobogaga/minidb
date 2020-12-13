package storage

import (
	"encoding/binary"
)

type Storage struct {
	Dbs map[string]DbInfo
}

func (storage Storage) HasSchema(schema string) bool {
	_, ok := storage.Dbs[schema]
	return ok
}

func (storage Storage) GetDbInfo(schema string) DbInfo {
	return storage.Dbs[schema]
}

var storage = Storage{}

func GetStorage() Storage {
	return storage
}

type DbInfo struct {
	Name   string
	Tables map[string]TableInfo
}

func (dbs DbInfo) HasTable(table string) bool {
	_, ok := dbs.Tables[table]
	return ok
}

func (dbs DbInfo) GetTable(table string) TableInfo {
	return dbs.Tables[table]
}

type TableInfo struct {
	Schema Schema
	Datas  []ColumnVector
}

// This is for join, a schema might have multiple sub table schemas.
type Schema struct {
	Tables []SingleTableSchema
	// Name   string // Because this can support join tables. do we need name?
}

// A SingleTableSchema is a list of Fields representing a temporal table format.
// It can has multiple columns, each column has a DatabaseName, TableName, ColumnName to allow
// multiple columns coexist with same columnName but are from different database.
type SingleTableSchema struct {
	Columns    []Field
	TableName  string
	SchemaName string
}

// FetchData returns the data starting at row index `rowIndex` and the batchSize is batchSize.
func (table TableInfo) FetchData(rowIndex, batchSize int) *RecordBatch {
	ret := &RecordBatch{
		Fields:  table.Schema.Tables[0].Columns,
		Records: make([]ColumnVector, len(table.Schema.Tables[0].Columns)),
	}
	if len(table.Datas) == 0 {
		return ret
	}
	for i := rowIndex; i < batchSize && i < table.Datas[0].Size(); i++ {
		for j, col := range table.Datas {
			ret.Records[j].Append(col.Values[i])
		}
	}
	return ret
}

func (schema Schema) HasSubTable(tableName string) bool {
	for _, table := range schema.Tables {
		if table.TableName == tableName {
			return true
		}
	}
	return false
}

// HasColumn returns whether this schema has such schema, table and column.
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
	ret := schema // Are we safe here.
	ret.Tables = append(ret.Tables, right.Tables...)
	return ret, nil
}

type RecordBatch struct {
	Fields  []Field
	Records []ColumnVector
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
		Fields: make([]Field, len(recordBatch.Fields) + len(another.Fields)),
		Records: make([]ColumnVector, len(recordBatch.Records) + len(another.Records)),
	}
	// set Field first.
	for i, f := range recordBatch.Fields {
		ret.Fields[i] = f
	}
	j := len(recordBatch.Fields)
	for i, f := range another.Fields {
		ret.Fields[j + i] = f
	}
	// set column vector.
	for i, col := range recordBatch.Records {
		ret.Records[i] = col
	}
	j = len(recordBatch.Fields)
	for i, col := range another.Records {
		ret.Records[i + j] = col
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
// whose field is Field{Name: "order", TP: storage.Int}.
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
	recordBatch.CopyFrom(temp, 0, temp.RowCount())
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

func (recordBatch *RecordBatch) CopyFrom(copyFrom *RecordBatch, from, size int) *RecordBatch {

}

// selectedRows is a bool column which represent each row in recordBatch is selected or not.
func (recordBatch *RecordBatch) Filter(selectedRows ColumnVector) *RecordBatch {

}

// Return data[startIndex: startIndex + size - 1]
func (recordBatch *RecordBatch) Slice(startIndex, size int) *RecordBatch {

}

// Encode row key.
func (recordBatch *RecordBatch) RowKey(row int) []byte {

}

// For type check.
type Field struct {
	TP         FieldTP
	Name       string
	TableName  string
	SchemaName string
}

func (f Field) CanEqual(another Field) bool {

}

func (f Field) CanCompare(another Field) bool {

}

func (f Field) IsFieldNumerialType() bool {
	return f.TP == Int || f.TP == BigInt || f.TP == Float
}

func (f Field) CanLogicOp(another Field) bool {

}

func (f Field) IsComparable() bool {

}

// Return whether f can be cascaded to tp
func (f Field) CanCascadeTo(tp FieldTP) bool {

}

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
)

func (f Field) InferenceType(another Field, op OpType) FieldTP {

}

func InferenceType(data []byte) Field {

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

func (column ColumnVector) Negative() ColumnVector {
	// column must be a numeric type
	ret := ColumnVector{Field: column.Field}
	for _, value := range column.Values {
		v, _ := negative(column.Field.TP, value)
		ret.Values = append(ret.Values, v)
	}
	return ret
}

func (column ColumnVector) Add(another ColumnVector) ColumnVector {

}

func (column ColumnVector) Minus(another ColumnVector) ColumnVector {

}

func (column ColumnVector) Mul(another ColumnVector) ColumnVector {

}

func (column ColumnVector) Divide(another ColumnVector) ColumnVector {}

func (column ColumnVector) Mod(another ColumnVector) ColumnVector {}

func (column ColumnVector) Equal(another ColumnVector) ColumnVector {}

func (column ColumnVector) Is(another ColumnVector) ColumnVector {}

func (column ColumnVector) NotEqual(another ColumnVector) ColumnVector {}

func (column ColumnVector) Great(another ColumnVector) ColumnVector {}

func (column ColumnVector) GreatEqual(another ColumnVector) ColumnVector {}

func (column ColumnVector) Less(another ColumnVector) ColumnVector {}

func (column ColumnVector) LessEqual(another ColumnVector) ColumnVector {}

func (column ColumnVector) And(another ColumnVector) ColumnVector {}

func (column ColumnVector) Or(another ColumnVector) ColumnVector {}

func (column ColumnVector) Sort(others []ColumnVector, asc []bool) ColumnVector {

}

func (column ColumnVector) Bool(row int) bool {

}

func (column ColumnVector) Int(row int) int {

}

func (column ColumnVector) Append(value []byte) {

}

func (column ColumnVector) Appends(values [][]byte) {

}

type RowValue struct {
	Field Field
	Data  []byte
}

func bytesToTp(value []byte) FieldTP {

}

func valueToBytes() {

}

func negative(tp FieldTP, value []byte) ([]byte, error) {
	switch tp {
	case Int:
		v := Decode(tp, value)
		return Encode(tp, -(v.(uint64)))
	case Float:

	default:

	}
}

func Encode(tp FieldTP, value interface{}) ([]byte, error) {
	switch tp {
	case Bool:
		if value.(bool) {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case Int:
		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutUvarint(buf, value.(uint64))
		return buf, nil
	case Float:
		f := value.(float64)
		bits := math.
			binary.BigEndian
	}
}

func Decode(tp FieldTP, value []byte) interface{} {
	switch tp {
	case Bool:
		return value[0] == 1
	case Int:
		v := binary.BigEndian.Uint64(value)
		return v
	case Float:

	}
}

type FieldTP string

const (
	Bool       FieldTP = "bool"
	Int        FieldTP = "int"
	BigInt     FieldTP = "bigint"
	Float      FieldTP = "float"
	Char       FieldTP = "char"
	VarChar    FieldTP = "varchar"
	DateTime   FieldTP = "datetime"
	Blob       FieldTP = "blob"
	MediumBlob FieldTP = "mediumBlob"
	Text       FieldTP = "text"
	MediumText FieldTP = "mediumText"
	Constant   FieldTP = "constant" // like numeric or string or char value.
)
