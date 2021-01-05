package plan

import (
	"errors"
	"fmt"
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
	return FillShowPlanData(stm, currentDB)
}

func FillShowPlanData(stm *parser.ShowStm, currentDB string) (*storage.RecordBatch, error) {
	if stm.TP == parser.ShowCreateTableTP {
		return FillShowCreateTableData(stm, currentDB)
	}
	name := "tables"
	if stm.TP == parser.ShowDatabaseTP {
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
	switch stm.TP {
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
	return ret, nil
}

func FillShowCreateTableData(stm *parser.ShowStm, currentDB string) (*storage.RecordBatch, error) {
	dbName, tableName, err := getSchemaTableName(stm.Table, currentDB)
	if err != nil {
		return nil, err
	}
	dbInfo := storage.GetStorage().GetDbInfo(dbName)
	if dbInfo == nil {
		return nil, errors.New(fmt.Sprintf("cannot find such db: '%s'", dbName))
	}
	tbInfo := dbInfo.GetTable(tableName)
	if tbInfo == nil {
		return nil, errors.New(fmt.Sprintf("cannot find such table: '%s.%s'", dbName, tableName))
	}
	return tbInfo.Describe(), nil
}
