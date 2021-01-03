package storage

import (
	"bytes"
	"fmt"
	"testing"
)

func printColumn(t *testing.T, col Field, padding string) {
	fmt.Printf("%sColumn: %s.%s.%s, TP: %s, primaryKey: %v, allowNull: %v, autoInc: %v.\n", padding, col.SchemaName,
		col.TableName, col.Name, col.TP.Name, col.PrimaryKey, col.AllowNull, col.AutoIncrement)
}

func printTableRowData(t *testing.T, tableInfo *TableInfo, row int, padding string) {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("%s", padding))
	for i, col := range tableInfo.Datas {
		if i == 0 {
			continue
		}
		buf.WriteString(col.String(row) + ", ")
	}
	println(buf.String())
}

func printTableHeader(t *testing.T, tableInfo *TableInfo, padding string) {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("%s", padding))
	for i, col := range tableInfo.Datas {
		if i == 0 {
			continue
		}
		// Wont print row index column.
		buf.WriteString(col.Field.Name + " ,")
	}
	println(buf.String())
}

func printTable(t *testing.T, tableInfo *TableInfo, padding string) {
	fmt.Printf("%sTable: %s.%s, collate: %s, charset: %s.\n", padding, tableInfo.TableSchema.SchemaName(),
		tableInfo.TableSchema.TableName(), tableInfo.Collate, tableInfo.Charset)
	// Now we can print table column definitions.
	for _, col := range tableInfo.TableSchema.Columns {
		printColumn(t, col, padding+padding)
	}
	fmt.Printf("%sTable data:\n", padding)
	printTableHeader(t, tableInfo, padding+padding)
	// Now print Test data
	for i := 0; i < tableInfo.Datas[1].Size(); i++ {
		printTableRowData(t, tableInfo, i, padding+padding)
	}
	println("")
}

func PrintStorage(t *testing.T) {
	storage := GetStorage()
	for _, dbSchema := range storage.Dbs {
		fmt.Printf("dbInfo: %s, collate: %s, charset: %s.\n", dbSchema.Name, dbSchema.Collate, dbSchema.Charset)
		for _, table := range dbSchema.Tables {
			printTable(t, table, "\t")
		}
	}
}

func printRecordBatchHeader(record *RecordBatch, on bool) {
	if !on {
		return
	}
	buf := bytes.Buffer{}
	for i := 0; i < len(record.Fields); i++ {
		buf.WriteString(record.Fields[i].Name + ",")
	}
	println(buf.String())
}

func printRecordBatchRowData(record *RecordBatch, row int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		buf.WriteString(record.Records[i].String(row) + ",")
	}
	println(buf.String())
}

func PrintRecordBatch(record *RecordBatch, on bool) {
	// Print header first.
	if record == nil {
		return
	}
	printRecordBatchHeader(record, on)
	for i := 0; i < record.RowCount(); i++ {
		printRecordBatchRowData(record, i)
	}
	println()
}
