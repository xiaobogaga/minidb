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
	if stm.TP == parser.ShowTableTP && currentDB == "" {
		return nil, errors.New("please select db first")
	}
	show.Done = true
	return FillShowPlanData(stm.TP, currentDB), nil
}

func FillShowPlanData(tp parser.ShowStmTp, currentDB string) *storage.RecordBatch {
	name := "tables"
	if tp == parser.ShowDatabaseTP {
		name = "databases"
	}
	ret := &storage.RecordBatch{
		Fields: []storage.Field{
			storage.RowIndexField("", ""),
			{TP: storage.DefaultFieldTpMap[storage.Text], Name: name},
		},
		Records: []*storage.ColumnVector{{}, {}},
	}
	ret.Records[0].Field, ret.Records[1].Field = ret.Fields[0], ret.Fields[1]
	switch tp {
	case parser.ShowDatabaseTP:
		i := 0
		for db := range storage.GetStorage().Dbs {
			ret.Records[0].Append(storage.EncodeInt(int64(i)))
			ret.Records[1].Append([]byte(db))
			i++
		}
	case parser.ShowTableTP:
		dbInfo := storage.GetStorage().GetDbInfo(currentDB)
		i := 0
		for table := range dbInfo.Tables {
			ret.Records[0].Append(storage.EncodeInt(int64(i)))
			ret.Records[1].Append([]byte(table))
			i++
		}
	default:
		panic("unknown show type")
	}
	return ret
}
