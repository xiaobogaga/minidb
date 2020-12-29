package plan

import (
	"errors"
	"minidb/parser"
	"minidb/storage"
)

type Show struct {
	Done bool
}

func (show *Show) Execute(currentDB string, stm *parser.ShowStm) (*storage.RecordBatch, error) {
	if show.Done {
		return nil, nil
	}
	defer func() {
		show.Done = true
	}()
	switch stm.TP {
	case parser.ShowTableTP:
		// Prepare show table resp format.
		// | rowId | tables |
		if currentDB == "" {
			return nil, errors.New("please select db first")
		}
		ret := &storage.RecordBatch{
			Fields: []storage.Field{
				storage.RowIndexField("", ""),
				{TP: storage.Text, Name: "tables"},
			},
			Records: []*storage.ColumnVector{
				{},
				{},
			},
		}
		ret.Records[0].Field, ret.Records[1].Field = ret.Fields[0], ret.Fields[1]
		dbInfo := storage.GetStorage().GetDbInfo(currentDB)
		i := 0
		for table := range dbInfo.Tables {
			ret.Records[0].Append(storage.EncodeInt(int64(i)))
			ret.Records[1].Append([]byte(table))
			i++
		}
		return ret, nil
	case parser.ShowDatabaseTP:
		// Prepare show database resp format
		// | rowId | databases |
		ret := &storage.RecordBatch{
			Fields: []storage.Field{
				storage.RowIndexField("", ""),
				{TP: storage.Text, Name: "databases"},
			},
			Records: []*storage.ColumnVector{
				{},
				{},
			},
		}
		ret.Records[0].Field, ret.Records[1].Field = ret.Fields[0], ret.Fields[1]
		i := 0
		for db := range storage.GetStorage().Dbs {
			ret.Records[0].Append(storage.EncodeInt(int64(i)))
			ret.Records[1].Append([]byte(db))
			i++
		}
		return ret, nil
	default:
		panic("unknown show tp")
	}
}
