package plan

import (
	"errors"
	"minidb/parser"
	"minidb/storage"
	"strings"
)

type Executor struct {
	Plan      interface{}
	Stm       parser.Stm
	CurrentDB *string
}

func MakeExecutor(stm parser.Stm, currentDB *string) (*Executor, error) {
	exec := &Executor{Stm: stm, CurrentDB: currentDB}
	switch stm.(type) {
	case *parser.SelectStm:
		ret, err := MakeSelectPlan(stm.(*parser.SelectStm), *currentDB)
		if err != nil {
			return nil, err
		}
		exec.Plan = ret
	case *parser.ShowStm:
		exec.Plan = &Show{}
	default:
	}
	return exec, nil
}

func (exec *Executor) Exec() (data *storage.RecordBatch, err error) {
	currentDB := *exec.CurrentDB
	stm := exec.Stm
	switch stm.(type) {
	case *parser.CreateDatabaseStm:
		return nil, ExecuteCreateDatabaseStm(stm.(*parser.CreateDatabaseStm))
	case *parser.DropDatabaseStm:
		return nil, ExecuteDropDatabaseStm(stm.(*parser.DropDatabaseStm))
	case *parser.CreateTableStm:
		return nil, ExecuteCreateTableStm(stm.(*parser.CreateTableStm), currentDB)
	case *parser.DropTableStm:
		return nil, ExecuteDropTableStm(stm.(*parser.DropTableStm), currentDB)
	case *parser.InsertIntoStm:
		return nil, ExecuteInsertStm(stm.(*parser.InsertIntoStm), currentDB)
	case *parser.UpdateStm:
		return nil, ExecuteUpdateStm(stm.(*parser.UpdateStm), currentDB)
	case *parser.MultiUpdateStm:
		return nil, ExecuteMultiUpdateStm(stm.(*parser.MultiUpdateStm), currentDB)
	case *parser.SingleDeleteStm:
		return nil, ExecuteDeleteStm(stm.(*parser.SingleDeleteStm), currentDB)
	case *parser.MultiDeleteStm:
		return nil, ExecuteMultiDeleteStm(stm.(*parser.MultiDeleteStm), currentDB)
	case *parser.TruncateStm:
		return nil, ExecuteTruncateStm(stm.(*parser.TruncateStm), currentDB)
	case *parser.SelectStm:
		data = exec.Plan.(LogicPlan).Execute()
		return data, nil
	case *parser.ShowStm:
		data, err = exec.Plan.(*Show).Execute(currentDB, stm.(*parser.ShowStm))
		return data, err
	case *parser.UseDatabaseStm:
		err = ExecuteUseStm(stm.(*parser.UseDatabaseStm))
		if err == nil {
			*exec.CurrentDB = stm.(*parser.UseDatabaseStm).DatabaseName
		}
		return nil, nil
	default:
		return nil, errors.New("unsupported statement")
	}
}

func MakeSelectPlan(stm *parser.SelectStm, currentDB string) (LogicPlan, error) {
	// we need to generate a logic plan for this selectStm.
	plan, err := MakeLogicPlan(stm, currentDB)
	if err != nil {
		return nil, err
	}
	return plan, nil
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
		return errors.New("database doesn't exist")
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
		table = arr[0]
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

func getSchema(stm *parser.CreateTableStm, dbInfo *storage.DbInfo) (*storage.TableSchema, error) {
	schemaName, tableName, _ := getSchemaTableName(stm.TableName, dbInfo.Name)
	ret := &storage.TableSchema{
		Columns: make([]storage.Field, len(stm.Cols)+1),
	}
	// Add row index field.
	ret.Columns[0] = storage.RowIndexField(schemaName, tableName)
	hasPrimaryColumn := false
	for i, colDef := range stm.Cols {
		col := columnDefToStorageColumn(colDef, tableName, schemaName)
		if hasPrimaryColumn && col.PrimaryKey {
			return ret, errors.New("multi primary key defined")
		}
		hasPrimaryColumn = hasPrimaryColumn || col.PrimaryKey
		ret.Columns[i+1] = col
	}
	//if !hasPrimaryColumn {
	//	// Add the default primary key column to the table.
	//	primaryKeyCol := storage.DefaultPrimaryKeyColumn(schemaName, tableName)
	//	ret.Columns = append([]storage.Field{primaryKeyCol}, ret.Columns...)
	//}
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
	if dbInfo == nil {
		return errors.New("cannot find such db")
	}
	tableSchema, err := getSchema(stm, dbInfo)
	if err != nil {
		return err
	}
	table := &storage.TableInfo{
		TableSchema: tableSchema,
		Charset:     stm.Charset,
		Collate:     stm.Collate,
		Engine:      stm.Engine,
		Datas:       make([]*storage.ColumnVector, len(tableSchema.Columns)),
	}
	for i, col := range table.TableSchema.Columns {
		table.Datas[i] = &storage.ColumnVector{Field: col}
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
		if dbInfo == nil || !dbInfo.HasTable(tableName) {
			return errors.New("cannot found such table")
		}
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

func ExecuteUseStm(stm *parser.UseDatabaseStm) error {
	if storage.GetStorage().GetDbInfo(stm.DatabaseName) != nil {
		return nil
	}
	return errors.New("schema doesn't found")
}
