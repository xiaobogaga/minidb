package storage

import (
	"encoding/json"
	"testing"
)

func TestRecordBatch_JsonEncode(t *testing.T) {
	record := RecordBatch{
		Fields:  []Field{RowIndexField, {Name: "id", TP: Int, PrimaryKey: true}},
		Records: make([]ColumnVector, 2),
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
