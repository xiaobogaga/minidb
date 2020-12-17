package plan

import (
	"errors"
	"simpleDb/parser"
	"simpleDb/storage"
	"strings"
)

func Exec(stm parser.Stm, currentDB string) {

}

func ExecuteSelectStm(stm *parser.SelectStm, currentDB string) error {
	// we need to generate a logic plan for this selectStm.
	logicPlan, err := MakeLogicPlan(stm, currentDB)
	if err != nil {
		return err
	}
	// physicalPlan := plan.MakePhysicalPlan(logicPlan)
	for {
		data := logicPlan.Execute()
		if data == nil {
			// means we have all data
			return nil
		}
		// Todo: send data to client.
	}
	return nil
}

func ExecuteCreateDatabaseStm(stm *parser.CreateDatabaseStm) error {
	if stm.IfNotExist && storage.GetStorage().HasSchema(stm.DatabaseName) {
		return nil
	}
	// Create database otherwise
	storage.GetStorage().CreateSchema(stm.DatabaseName, stm.Charset, stm.Collate)
	return nil
}

func ExecuteRemoveDatabaseStm(stm *parser.DropDatabaseStm) error {
	if !storage.GetStorage().HasSchema(stm.DatabaseName) {
		return nil
	}
	storage.GetStorage().RemoveSchema(stm.DatabaseName)
	return nil
}

func getSchemaTableName(schemaTable string, defaultSchemaName string) (schema string, table string, err error) {
	arr := strings.Split(schemaTable, ".")
	switch len(arr) {
	case 2:
		schema = arr[0]
		table = arr[1]
	case 1:
		schema = defaultSchemaName
	default:
		err = errors.New("wrong table name format")
	}
	return
}

func columnTypeToFieldType(col parser.ColumnType) storage.FieldTP {
	// Todo: support column range.
	switch col.Tp {
	case parser.BOOL:
		return storage.Bool
	case parser.INT:
		return storage.Int
	case parser.FLOAT:
		return storage.Float
	case parser.CHAR:
		return storage.Char
	case parser.VARCHAR:
		return storage.VarChar
	case parser.TEXT:
		return storage.Text
	case parser.MEDIUMTEXT:
		return storage.MediumText
	case parser.BLOB:
		return storage.Blob
	case parser.MEDIUMBLOB:
		return storage.MediumBlob
	case parser.DATETIME:
		return storage.DateTime
	default:
		panic("unknown col type")
	}
}

func columnDefToStorageColumn(col *parser.ColumnDefStm, tableName, schemaName string) storage.Field {
	ret := storage.Field{
		TP:            columnTypeToFieldType(col.ColumnType),
		Name:          col.ColName,
		TableName:     tableName,
		SchemaName:    schemaName,
		AllowNull:     col.AllowNULL,
		AutoIncrement: col.AutoIncrement,
	}
	return ret
}

func getSchema(stm *parser.CreateTableStm, dbInfo *storage.DbInfo) storage.Schema {
	schemaName, tableName, _ := getSchemaTableName(stm.TableName, dbInfo.Name)
	ret := storage.Schema{
		Tables: make([]storage.SingleTableSchema, 1),
	}
	ret.Tables[0] = storage.SingleTableSchema{
		Columns:    make([]storage.Field, len(stm.Cols)),
		TableName:  tableName,
		SchemaName: schemaName,
	}
	for i, col := range stm.Cols {
		ret.Tables[0].Columns[i] = columnDefToStorageColumn(col, tableName, schemaName)
	}
	return ret
}

func ExecuteCreateTableStm(stm *parser.CreateTableStm, currentDB string) error {
	schemaName, tableName, err := getSchemaTableName(stm.TableName, currentDB)
	if err != nil {
		return err
	}
	if stm.IfNotExist && storage.GetStorage().HasTable(schemaName, tableName) {
		return nil
	}
	dbInfo := storage.GetStorage().GetDbInfo(schemaName)
	table := &storage.TableInfo{
		Schema:  getSchema(stm, dbInfo),
		Charset: stm.Charset,
		Collate: stm.Collate,
		Engine:  stm.Engine,
	}
	dbInfo.AddTable(table)
	return nil
}

func ExecuteDropTableStm(stm *parser.DropTableStm, currentDB string) error {
	for _, table := range stm.TableNames {
		schemaName, tableName, err := getSchemaTableName(table, currentDB)
		if err != nil {
			return err
		}
		dbInfo := storage.GetStorage().GetDbInfo(schemaName)
		dbInfo.RemoveTable(tableName)
	}
	return nil
}

func ExecuteInsertStm(stm *parser.InsertIntoStm, currentDB string) error {
	schemaName, tableName, err := getSchemaTableName(stm.TableName, currentDB)
	if err != nil {
		return err
	}
	plan := MakeInsertPlan(tableName, schemaName, stm.Cols, stm.Values)
	err = plan.TypeCheck()
	if err != nil {
		return err
	}
	return plan.Execute()
}

func ExecuteUpdateStm(stm *parser.UpdateStm, currentDB string) error {
	if len(stm.TableRefs) > 1 {
		return ExecuteMultiUpdateStm(stm, currentDB)
	}
	tableRef := stm.TableRefs[0].TableReference.(parser.TableReferenceTableFactorStm).TableFactorReference.(parser.TableReferencePureTableRefStm)
	schemaName, tableName, err := getSchemaTableName(tableRef.TableName, currentDB)
	if err != nil {
		return err
	}
	MakeUpdatePlan(schemaName, tableName)
}

func ExecuteMultiUpdateStm(stm *parser.UpdateStm, currentDB string) error {
}

func ExecuteDeleteStm(stm *parser.SingleDeleteStm, currentDB string) error {

}

func ExecuteMultiDeleteStm(stm *parser.MultiDeleteStm, currentDB string) error {}

func ExecuteTruncateStm(stm *parser.TruncateStm) error {}

func ExecuteAlterStm(stm interface{}) error {
	return errors.New("unsupported statement")
}
