package storage

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecordBatch_JsonEncode(t *testing.T) {
	record := RecordBatch{
		Fields:  []Field{RowIndexField, {Name: "id", TP: Int, PrimaryKey: true}},
		Records: make([]*ColumnVector, 2),
	}
	record.Records[0].Field = record.Fields[0]
	record.Records[1].Field = record.Fields[1]
	for i := 0; i < 2; i++ {
		record.Records[0].Append(EncodeInt(int64(i)))
		record.Records[1].Append(EncodeInt(int64(i)))
	}
	data, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	jrecord := &RecordBatch{}
	err = json.Unmarshal(data, jrecord)
	if err != nil {
		panic(err)
	}
}

func TestInferenceType(t *testing.T) {
	data := []byte("\"hello\"")
	assert.Equal(t, Text, InferenceType(data))
	data = []byte("10.0")
	assert.Equal(t, Float, InferenceType(data))
	data = []byte("100")
	assert.Equal(t, Int, InferenceType(data))
	data = []byte("true")
	assert.Equal(t, Bool, InferenceType(data))
}

func TestField_CanOp(t *testing.T) {
	f := Field{TP: Int}
	another := Field{TP: Float}
	assert.Nil(t, f.CanOp(another, AddOpType))
	assert.Nil(t, another.CanOp(f, AddOpType))
	assert.Nil(t, f.CanOp(f, NegativeOpType))
	assert.Nil(t, another.CanOp(another, NegativeOpType))
	textField := Field{TP: Text}
	assert.NotNil(t, f.CanOp(textField, AddOpType))
	assert.Nil(t, f.CanOp(f, ModOpType))
	assert.NotNil(t, another.CanOp(another, ModOpType))
	boolField := Field{TP: Bool}
	assert.Nil(t, boolField.CanOp(boolField, AndOpType))
	assert.NotNil(t, boolField.CanOp(f, AndOpType))
}

func TestField_InferenceType(t *testing.T) {
	intF := Field{TP: Int}
	floatF := Field{TP: Float}
	boolF := Field{TP: Bool}
	assert.Equal(t, Int, intF.InferenceType(intF, AddOpType))
	assert.Equal(t, Float, intF.InferenceType(floatF, AddOpType))
	assert.Equal(t, Float, floatF.InferenceType(floatF, AddOpType))
	assert.Equal(t, Bool, boolF.InferenceType(boolF, AndOpType))
	assert.Equal(t, Bool, intF.InferenceType(floatF, GreatEqualOpType))
}

func makeSchemaForTesting(dbName string, tableName string, fieldNames []string, fieldsTP []FieldTP) *Schema {
	schema := &Schema{
		Tables: []*SingleTableSchema{
			{
				SchemaName: dbName,
				TableName:  tableName,
				Columns:    make([]Field, len(fieldNames)),
			},
		},
	}
	for i := 0; i < len(fieldNames); i++ {
		schema.Tables[0].Columns[i] = Field{
			TP:         fieldsTP[i],
			Name:       fieldNames[i],
			SchemaName: dbName,
			TableName:  tableName,
		}
	}
	return schema
}

func TestSchema_HasAmbiguousColumn(t *testing.T) {
	schema := makeSchemaForTesting("test", "people", []string{"id", "name"}, []FieldTP{Int, VarChar})
	assert.False(t, schema.HasAmbiguousColumn("", "", "id"))
	assert.True(t, schema.HasColumn("", "", "id"))
	assert.True(t, schema.HasSubTable("people"))
	another := makeSchemaForTesting("test2", "people2", []string{"id", "location"}, []FieldTP{Int, VarChar})
	mergedSchema, _ := schema.Merge(another)
	assert.True(t, mergedSchema.HasSubTable("people2"))
	assert.True(t, mergedSchema.HasAmbiguousColumn("", "", "id"))
	assert.False(t, mergedSchema.HasAmbiguousColumn("test2", "", "id"))
	assert.False(t, mergedSchema.HasAmbiguousColumn("", "people", "id"))
	assert.True(t, mergedSchema.HasColumn("", "", "id"))
	assert.True(t, mergedSchema.HasColumn("", "people", "id"))
	assert.True(t, mergedSchema.HasColumn("test2", "", "id"))
	assert.False(t, mergedSchema.HasColumn("", "", "id2"))
}

func makeRecordBatchForTesting(recordSize int) *RecordBatch {
	fields := []Field{
		{TP: Int, Name: "id"},
		{TP: VarChar, Name: "name"},
	}
	ret := &RecordBatch{
		Fields:  fields,
		Records: make([]*ColumnVector, len(fields)),
	}
	for i := 0; i < len(fields); i++ {
		ret.Records[i] = &ColumnVector{Field: fields[i]}
	}
	for i := 0; i < recordSize; i++ {
		ret.Records[0].Append(EncodeInt(int64(i)))
		ret.Records[1].Append([]byte(fmt.Sprintf("name: %d", i)))
	}
	return ret
}

func TestRecordBatch_Slice(t *testing.T) {
	recordBatch := makeRecordBatchForTesting(4)
	ret := recordBatch.Slice(1, 2)
	assert.Equal(t, 2, ret.RowCount())
	assert.Equal(t, 2, ret.ColumnCount())
	assert.Equal(t, int64(1), ret.Records[0].Int(0))
	assert.Equal(t, int64(2), ret.Records[0].Int(1))
}

func TestRecordBatch_Filter(t *testing.T) {
	recordBatch := makeRecordBatchForTesting(3)
	// select 0, 2 row.
	selectedRow := ColumnVector{
		Field: Field{TP: Bool},
		Values: [][]byte{
			EncodeBool(true),
			EncodeBool(false),
			EncodeBool(true),
		},
	}
	ret := recordBatch.Filter(selectedRow)
	assert.Equal(t, 2, ret.RowCount())
	assert.Equal(t, 2, ret.ColumnCount())
	assert.Equal(t, int64(0), ret.Records[0].Int(0))
	assert.Equal(t, int64(2), ret.Records[0].Int(1))
}

func TestRecordBatch_Join(t *testing.T) {
	//record1 := makeRecordBatchForTesting(3)
	//record2 := makeRecordBatchForTesting(2)
	//ret := record1.Join(record2)
	// Todo
}

func TestRecordBatch_OrderBy(t *testing.T) {
	record := makeRecordBatchForTesting(3)
	orderByCol := ColumnVector{
		Field: Field{TP: Int},
		Values: [][]byte{
			EncodeInt(2),
			EncodeInt(1),
			EncodeInt(0),
		},
	}
	record.OrderBy(orderByCol)
	assert.Equal(t, 3, record.RowCount())
	assert.Equal(t, 2, record.ColumnCount())
	assert.Equal(t, int64(2), record.Records[0].Int(0))
	assert.Equal(t, int64(1), record.Records[0].Int(1))
	assert.Equal(t, int64(0), record.Records[0].Int(2))
}

func TestColumnVector_Add(t *testing.T) {
	intC := &ColumnVector{
		Field: Field{TP: Int},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(2),
		},
	}
	ret := intC.Add(intC, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, Int, ret.GetTP())
	assert.Equal(t, int64(2), ret.Int(0))
	assert.Equal(t, int64(4), ret.Int(1))
}

func TestColumnVector_Equal(t *testing.T) {
	textF := &ColumnVector{
		Field: Field{TP: Text},
		Values: [][]byte{
			[]byte("hello"),
			[]byte("hi"),
		},
	}
	ret := textF.Equal(textF, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, Bool, ret.GetTP())
	assert.Equal(t, true, ret.Bool(0))
	assert.Equal(t, true, ret.Bool(1))
	anotherF := &ColumnVector{
		Field: Field{TP: Text},
		Values: [][]byte{
			[]byte("hello"),
			[]byte("hi2"),
		},
	}
	ret = textF.Equal(anotherF, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, Bool, ret.GetTP())
	assert.Equal(t, true, ret.Bool(0))
	assert.Equal(t, false, ret.Bool(1))
}

func TestColumnVector_And(t *testing.T) {
	textF := &ColumnVector{
		Field: Field{TP: Bool},
		Values: [][]byte{
			EncodeBool(true),
			EncodeBool(false),
		},
	}
	ret := textF.And(textF, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, Bool, ret.GetTP())
	assert.Equal(t, true, ret.Bool(0))
	assert.Equal(t, false, ret.Bool(1))
}

func TestColumnVector_Sort(t *testing.T) {
	intF := &ColumnVector{
		Field: Field{TP: Int},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(2),
			EncodeInt(-1),
			EncodeInt(0),
		},
	}
	textF := &ColumnVector{
		Field: Field{TP: Text},
		Values: [][]byte{
			[]byte("1"),
			[]byte("2"),
			[]byte("-1"),
			[]byte("0"),
		},
	}
	ret := textF.Sort([]*ColumnVector{intF}, []bool{true})
	assert.Equal(t, 4, ret.Size())
	for i := 0; i < 4; i++ {
		assert.Equal(t, fmt.Sprintf("%d", i-1), ret.String(i))
	}

	intF1 := &ColumnVector{
		Field: Field{TP: Int},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(1),
			EncodeInt(-1),
			EncodeInt(0),
		},
	}
	intF2 := &ColumnVector{
		Field: Field{TP: Int},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(2),
			EncodeInt(-1),
			EncodeInt(0),
		},
	}
	ret = textF.Sort([]*ColumnVector{intF1, intF2}, []bool{true, false})
	assert.Equal(t, 4, ret.Size())
	for i := 0; i < 4; i++ {
		assert.Equal(t, fmt.Sprintf("%d", i-1), ret.String(i))
	}
}
