package plan

import (
	"errors"
	"simpleDb/parser"
	"simpleDb/storage"
	"strings"
)

func Exec(stm parser.Stm, currentDB string) (*storage.RecordBatch, error) {
	switch stm.(type) {
	case parser.CreateDatabaseStm:
		return nil, ExecuteCreateDatabaseStm(stm.(*parser.CreateDatabaseStm))
	case parser.DropDatabaseStm:
		return nil, ExecuteDropDatabaseStm(stm.(*parser.DropDatabaseStm))
	case parser.CreateTableStm:
		return nil, ExecuteCreateTableStm(stm.(*parser.CreateTableStm), currentDB)
	case parser.DropTableStm:
		return nil, ExecuteDropTableStm(stm.(*parser.DropTableStm), currentDB)
	case parser.InsertIntoStm:
		return nil, ExecuteInsertStm(stm.(*parser.InsertIntoStm), currentDB)
	case parser.UpdateStm:
		return nil, ExecuteUpdateStm(stm.(*parser.UpdateStm), currentDB)
	case parser.MultiUpdateStm:
		return nil, ExecuteMultiUpdateStm(stm.(*parser.MultiUpdateStm), currentDB)
	case parser.SingleDeleteStm:
		return nil, ExecuteDeleteStm(stm.(*parser.SingleDeleteStm), currentDB)
	case parser.MultiDeleteStm:
		return nil, ExecuteMultiDeleteStm(stm.(*parser.MultiDeleteStm), currentDB)
	case parser.TruncateStm:
		return nil, ExecuteTruncateStm(stm.(*parser.TruncateStm), currentDB)
	case parser.SelectStm:
		return ExecuteSelectStm(stm.(*parser.SelectStm), currentDB)
	default:
		return nil, errors.New("unsupported statement")
	}
}

func ExecuteSelectStm(stm *parser.SelectStm, currentDB string) (*storage.RecordBatch, error) {
	// we need to generate a logic plan for this selectStm.
	plan, err := MakeLogicPlan(stm, currentDB)
	if err != nil {
		return nil, err
	}
	return plan.Execute(), nil
}

func ExecuteCreateDatabaseStm(stm *parser.CreateDatabaseStm) error {
	if stm.IfNotExist && storage.GetStorage().HasSchema(stm.DatabaseName) {
		return nil
	}
	// Create database otherwise
	storage.GetStorage().CreateSchema(stm.DatabaseName, stm.Charset, stm.Collate)
	return nil
}

func ExecuteDropDatabaseStm(stm *parser.DropDatabaseStm) error {
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
		PrimaryKey:    col.PrimaryKey,
	}
	return ret
}

func getSchema(stm *parser.CreateTableStm, dbInfo *storage.DbInfo) (storage.Schema, error) {
	schemaName, tableName, _ := getSchemaTableName(stm.TableName, dbInfo.Name)
	ret := storage.Schema{
		Tables: make([]storage.SingleTableSchema, 1),
	}
	ret.Tables[0] = storage.SingleTableSchema{
		Columns:    make([]storage.Field, len(stm.Cols)+1),
		TableName:  tableName,
		SchemaName: schemaName,
	}
	// Add row index field.
	ret.Tables[0].Columns[0] = storage.RowIndexField
	hasPrimaryColumn := false
	for i, col := range stm.Cols {
		col := columnDefToStorageColumn(col, tableName, schemaName)
		if hasPrimaryColumn && col.PrimaryKey {
			return ret, errors.New("multi primary key defined")
		}
		hasPrimaryColumn = hasPrimaryColumn || col.PrimaryKey
		ret.Tables[0].Columns[i+1] = col
	}
	if !hasPrimaryColumn {
		// Add the default primary key column to the table.
		primaryKeyCol := storage.DefaultPrimaryKeyColumn()
		primaryKeyCol.TableName, primaryKeyCol.SchemaName = tableName, schemaName
		ret.Tables[0].Columns = append([]storage.Field{primaryKeyCol}, ret.Tables[0].Columns...)
	}
	return ret, nil
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
	tableSchema, err := getSchema(stm, dbInfo)
	if err != nil {
		return err
	}
	table := &storage.TableInfo{
		Schema:  tableSchema,
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
	plan := MakeInsertPlan(stm, currentDB)
	err := plan.TypeCheck()
	if err != nil {
		return err
	}
	return plan.Execute()
}

func ExecuteUpdateStm(stm *parser.UpdateStm, currentDB string) error {
	plan := MakeUpdatePlan(stm, currentDB)
	err := plan.TypeCheck()
	if err != nil {
		return err
	}
	return plan.Execute()
}

// For multi update statement, doesn't have orderBy, limit.
func ExecuteMultiUpdateStm(stm *parser.MultiUpdateStm, currentDB string) error {
	update := MakeMultiUpdatePlan(stm, currentDB)
	err := update.TypeCheck()
	if err != nil {
		return err
	}
	return update.Execute()
}

func ExecuteDeleteStm(stm *parser.SingleDeleteStm, currentDB string) error {
	plan := MakeDeletePlan(stm, currentDB)
	err := plan.TypeCheck()
	if err != nil {
		return err
	}
	return plan.Execute()
}

// For multi delete, there is no orderBy, no limit.
func ExecuteMultiDeleteStm(stm *parser.MultiDeleteStm, currentDB string) error {
	delete := MakeMultiDeletePlan(stm, currentDB)
	err := delete.TypeCheck()
	if err != nil {
		return err
	}
	return delete.Execute()
}

func ExecuteTruncateStm(stm *parser.TruncateStm, currentDB string) error {
	schemaName, tableName, err := getSchemaTableName(stm.TableName, currentDB)
	if err != nil {
		return err
	}
	if !storage.GetStorage().HasTable(schemaName, tableName) {
		return errors.New("table doesn't found")
	}
	dbInfo := storage.GetStorage().GetDbInfo(schemaName)
	dbInfo.GetTable(tableName).Truncate()
	return nil
}

func ExecuteAlterStm(stm interface{}) error {
	return errors.New("unsupported statement")
}
