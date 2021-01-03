package storage

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecordBatch_JsonEncode(t *testing.T) {
	record := RecordBatch{
		Fields: []Field{RowIndexField("test", "test1"), {Name: "id", TP: DefaultFieldTpMap[Int], PrimaryKey: true}},
		Records: []*ColumnVector{
			{},
			{},
		},
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
	assert.Equal(t, DefaultFieldTpMap[Text], InferenceType(data))
	data = []byte("10.0")
	assert.Equal(t, DefaultFieldTpMap[Float], InferenceType(data))
	data = []byte("100")
	assert.Equal(t, DefaultFieldTpMap[Int], InferenceType(data))
	data = []byte("true")
	assert.Equal(t, DefaultFieldTpMap[Bool], InferenceType(data))
}

func TestField_CanOp(t *testing.T) {
	f := Field{TP: DefaultFieldTpMap[Int]}
	another := Field{TP: DefaultFieldTpMap[Float]}
	assert.Nil(t, f.CanOp(another, AddOpType))
	assert.Nil(t, another.CanOp(f, AddOpType))
	assert.Nil(t, f.CanOp(f, NegativeOpType))
	assert.Nil(t, another.CanOp(another, NegativeOpType))
	textField := Field{TP: DefaultFieldTpMap[Text]}
	assert.NotNil(t, f.CanOp(textField, AddOpType))
	assert.Nil(t, f.CanOp(f, ModOpType))
	assert.NotNil(t, another.CanOp(another, ModOpType))
	boolField := Field{TP: DefaultFieldTpMap[Bool]}
	assert.Nil(t, boolField.CanOp(boolField, AndOpType))
	assert.NotNil(t, boolField.CanOp(f, AndOpType))
}

func TestField_InferenceType(t *testing.T) {
	intF := Field{TP: DefaultFieldTpMap[Int]}
	floatF := Field{TP: DefaultFieldTpMap[Float]}
	boolF := Field{TP: DefaultFieldTpMap[Bool]}
	assert.Equal(t, DefaultFieldTpMap[Int], intF.InferenceType(intF, AddOpType))
	assert.Equal(t, DefaultFieldTpMap[Float], intF.InferenceType(floatF, AddOpType))
	assert.Equal(t, DefaultFieldTpMap[Float], floatF.InferenceType(floatF, AddOpType))
	assert.Equal(t, DefaultFieldTpMap[Bool], boolF.InferenceType(boolF, AndOpType))
	assert.Equal(t, DefaultFieldTpMap[Bool], intF.InferenceType(floatF, GreatEqualOpType))
}

func makeSchemaForTesting(dbName string, tableName string, fieldNames []string, fieldsTP []FieldTP) *TableSchema {
	schema := &TableSchema{
		Columns: make([]Field, len(fieldNames)),
	}
	for i := 0; i < len(fieldNames); i++ {
		schema.Columns[i] = Field{
			TP:         fieldsTP[i],
			Name:       fieldNames[i],
			SchemaName: dbName,
			TableName:  tableName,
		}
	}
	return schema
}

func TestSchema_HasAmbiguousColumn(t *testing.T) {
	schema := makeSchemaForTesting("test", "people", []string{"id", "name"}, []FieldTP{DefaultFieldTpMap[Int], DefaultFieldTpMap[VarChar]})
	assert.False(t, schema.HasAmbiguousColumn("", "", "id"))
	assert.True(t, schema.HasColumn("", "", "id"))
	// assert.True(t, schema.HasSubTable("people"))
	another := makeSchemaForTesting("test2", "people2", []string{"id", "location"}, []FieldTP{DefaultFieldTpMap[Int], DefaultFieldTpMap[VarChar]})
	mergedSchema, _ := schema.Merge(another)
	// assert.True(t, mergedSchema.HasSubTable("people2"))
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
		{TP: DefaultFieldTpMap[Int], Name: "id"},
		{TP: DefaultFieldTpMap[VarChar], Name: "name"},
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
	selectedRow := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Text]},
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
	orderByCol := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Int]},
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
		Field: Field{TP: DefaultFieldTpMap[Int]},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(2),
		},
	}
	ret := intC.Add(intC, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, DefaultFieldTpMap[Int], ret.GetTP())
	assert.Equal(t, int64(2), ret.Int(0))
	assert.Equal(t, int64(4), ret.Int(1))
}

func TestColumnVector_Equal(t *testing.T) {
	textF := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Text]},
		Values: [][]byte{
			[]byte("hello"),
			[]byte("hi"),
		},
	}
	ret := textF.Equal(textF, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, DefaultFieldTpMap[Bool], ret.GetTP())
	assert.Equal(t, true, ret.Bool(0))
	assert.Equal(t, true, ret.Bool(1))
	anotherF := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Text]},
		Values: [][]byte{
			[]byte("hello"),
			[]byte("hi2"),
		},
	}
	ret = textF.Equal(anotherF, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, DefaultFieldTpMap[Bool], ret.GetTP())
	assert.Equal(t, true, ret.Bool(0))
	assert.Equal(t, false, ret.Bool(1))
}

func TestColumnVector_And(t *testing.T) {
	textF := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Bool]},
		Values: [][]byte{
			EncodeBool(true),
			EncodeBool(false),
		},
	}
	ret := textF.And(textF, "ret")
	assert.Equal(t, 2, ret.Size())
	assert.Equal(t, DefaultFieldTpMap[Bool], ret.GetTP())
	assert.Equal(t, true, ret.Bool(0))
	assert.Equal(t, false, ret.Bool(1))
}

func TestColumnVector_Sort(t *testing.T) {
	intF := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Int]},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(2),
			EncodeInt(-1),
			EncodeInt(0),
		},
	}
	textF := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Text]},
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
		Field: Field{TP: DefaultFieldTpMap[Int]},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(1),
			EncodeInt(-1),
			EncodeInt(0),
		},
	}
	intF2 := &ColumnVector{
		Field: Field{TP: DefaultFieldTpMap[Int]},
		Values: [][]byte{
			EncodeInt(1),
			EncodeInt(2),
			EncodeInt(-1),
			EncodeInt(0),
		},
	}
	ret = textF.Sort([]*ColumnVector{intF1, intF2}, []bool{true, false})
	assert.Equal(t, 4, ret.Size())
	assert.Equal(t, "-1", ret.String(0))
	assert.Equal(t, "0", ret.String(1))
	assert.Equal(t, "2", ret.String(2))
	assert.Equal(t, "1", ret.String(3))
	//	for i := 0; i < 4; i++ {
	//		assert.Equal(t, fmt.Sprintf("%d", i-1), ret.String(i))
	//	}
}

func TestTableInfo_FetchData(t *testing.T) {
	// Todo
}

func TestField_CanAssign(t *testing.T) {
	field := Field{TP: DefaultFieldTpMap[Int]}
	assert.Nil(t, field.CanAssign(EncodeInt(1)))
	field.TP.Name = Float
	assert.Nil(t, field.CanAssign(EncodeInt(1)))
	field.TP = FieldTP{Name: VarChar, Range: [2]int{10}}
	assert.NotNil(t, field.CanAssign([]byte("xxxxxxxxxxxxx")))
	field.TP = FieldTP{Name: DateTime}
	assert.Nil(t, field.CanAssign([]byte("2020-10-11 10:15:11")))
	field.TP = FieldTP{Name: Date}
	assert.Nil(t, field.CanAssign([]byte("2020-10-11")))
	assert.Nil(t, field.CanAssign([]byte("0003-10-11")))
	field.TP = FieldTP{Name: Time}
	assert.Nil(t, field.CanAssign([]byte("10:15:11")))
	// Several fail test.
	field.TP = FieldTP{Name: DateTime}
	assert.NotNil(t, field.CanAssign([]byte("2020-10-11 10:15:11x")))
	field.TP = FieldTP{Name: Date}
	assert.NotNil(t, field.CanAssign([]byte("2020-10-11x")))
	assert.NotNil(t, field.CanAssign([]byte("3-10-11x")))
	field.TP = FieldTP{Name: Time}
	assert.NotNil(t, field.CanAssign([]byte("10:15:11x")))
}

//func TestField_Cascade(t *testing.T) {
//	field := Field{TP: DefaultFieldTpMap[Int]}
//	assert.Equal(t, int64(1), DecodeInt(field.Cascade(EncodeInt(1))))
//	field.TP.Name = Float
//	field.TP.Range = [2]int{10, 2}
//	println(DecodeToString(field.Cascade(EncodeFloat(1111111111.1)), field.TP))
//}

func TestDecodeToString(t *testing.T) {
	val := EncodeFloat(10.20000)
	tp := FieldTP{Name: Float, Range: [2]int{10, 2}}
	str := DecodeToString(val, tp)
	assert.Equal(t, "10.20", str)
}
