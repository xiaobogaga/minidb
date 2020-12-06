package storage

import (
	"encoding/binary"
	"errors"
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

var storage Storage = Storage{}

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
}

// This is for join, a schema might have multiple sub table schemas.
type Schema struct {
	Tables []SingleTableSchema
	Name   string
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
	// Todo
}

func (schema Schema) HasSubTable(tableName string) bool {
	_, ok := schema.FieldMap[tableName]
	return ok
}

// HasColumn returns whether this schema has such schema, table and column.
// schemaName, tableName can be empty, then it will iterate all dbschema to find such column.
func (schema Schema) HasColumn(schemaName, tableName, columnName string) bool {
	if schemaName != "" {
		dbSchema, ok := schema.FieldMap[schemaName]
		if !ok {
			return false
		}
		tableSchema, ok := dbSchema[tableName]
		if !ok {
			return false
		}
		_, ok = tableSchema[columnName]
		return ok
	}
	if tableName != "" {
		for _, dbSchema := range schema.FieldMap {
			tableSchema, ok := dbSchema[tableName]
			if !ok {
				continue
			}
			_, ok = tableSchema[columnName]
			return ok
		}
		return false
	}
	for _, dbSchema := range schema.FieldMap {
		for _, tableSchema := range dbSchema {
			for colName, _ := range tableSchema {
				if colName == columnName {
					return true
				}
			}
		}
	}
	return false
}

func (schema Schema) HasAmbiguousColumn(schemaName, tableName, columnName string) bool {
	if schemaName != "" && tableName != "" {
		return false
	}
	i := 0
	if tableName == "" {
		for _, dbSchema := range schema.FieldMap {
			for _, tableSchema := range dbSchema {
				for colName, _ := range tableSchema {
					if colName == columnName {
						i++
					}
				}
			}
		}
		return i >= 2
	}
	i = 0
	// Then tableName mustn't be empty and schemaName must be empty.
	for _, dbSchema := range schema.FieldMap {
		tableSchema, ok := dbSchema[tableName]
		if !ok {
			continue
		}
		for colName, _ := range tableSchema {
			if colName == columnName {
				i++
			}
		}
	}
	return i >= 2
}

func (schema Schema) GetField(columnName string) Field {
	for _, dbSchema := range schema.FieldMap {
		for _, tableSchema := range dbSchema {
			for colName, f := range tableSchema {
				if colName == columnName {
					return f
				}
			}
		}
	}
	// Todo
	return Field{}
}

func (schema Schema) Merge(right Schema) (Schema, error) {
	ret := Schema{
		FieldMap: map[string]map[string]map[string]Field{},
	}
	for dbSchemaName, dbSchema := range schema.FieldMap {
		ret.AddNewDbTableSchema(dbSchemaName, dbSchema)
	}
	// Merge right schema. We doesn't allow to merge same table(means belongs to same database)
	for dbSchemaName, dbSchema := range right.FieldMap {
		retDbSchema, ok := ret.FieldMap[dbSchemaName]
		if !ok {
			ret.AddNewDbTableSchema(dbSchemaName, dbSchema)
			continue
		}
		for tableName, tableSchema := range dbSchema {
			retTableSchema, ok := retDbSchema[tableName]
			if ok {
				return Schema{}, errors.New("duplicate table name")
			}
			retTableSchema = map[string]Field{}
			for col, field := range tableSchema {
				retTableSchema[col] = field
			}
			retDbSchema[tableName] = retTableSchema
		}
	}
	return ret, nil
}

func (schema Schema) AddNewDbTableSchema(dbName string, dbSchema map[string]map[string]Field) {
	retDbSchema := map[string]map[string]Field{}
	for tableName, table := range dbSchema {
		retTableSchema := map[string]Field{}
		for col, field := range table {
			retTableSchema[col] = field
		}
		retDbSchema[tableName] = retTableSchema
	}
	schema.FieldMap[dbName] = retDbSchema
}

func (schema Schema) AddField() {

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

func (recordBatch *RecordBatch) GetColumnValue(colName string) ColumnVector {

}

func (recordBatch *RecordBatch) ColumnCount() int {
	return len(recordBatch.Records)
}

func (recordBatch *RecordBatch) Join(another *RecordBatch) *RecordBatch {}

// Append new to recordBatch, they are in the same layout.
func (recordBatch *RecordBatch) Append(new *RecordBatch) {}

func (recordBatch *RecordBatch) OrderBy(columnVector ColumnVector) {}

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

// Return data[startIndex: startIndex + size - 1]
func (recordBatch *RecordBatch) Slice(startIndex, size int) *RecordBatch {

}

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

func (column ColumnVector) BoolValue(row int) bool {

}

func (column ColumnVector) Append(value []byte) {

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
