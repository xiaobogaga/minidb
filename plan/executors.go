package plan

import (
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/parser"
	"github.com/xiaobogaga/minidb/storage"
	"github.com/xiaobogaga/minidb/util"
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
	case *parser.RenameStm:
		return nil, ExecuteRenameStm(stm.(*parser.RenameStm), currentDB)
	case *parser.TruncateStm:
		return nil, ExecuteTruncateStm(stm.(*parser.TruncateStm), currentDB)
	case *parser.SelectStm:
		data = exec.Plan.(Plan).Execute()
		return data, nil
	case *parser.ShowStm:
		data, err = exec.Plan.(*Show).Execute(currentDB, stm.(*parser.ShowStm))
		return data, err
	case *parser.UseDatabaseStm:
		err = ExecuteUseStm(stm.(*parser.UseDatabaseStm))
		if err == nil {
			*exec.CurrentDB = stm.(*parser.UseDatabaseStm).DatabaseName
		}
		return nil, err
	case parser.TransStm:
		return nil, errors.New("unsupported statement")
	default:
		return nil, errors.New("unsupported statement")
	}
}

func MakeSelectPlan(stm *parser.SelectStm, currentDB string) (Plan, error) {
	// we need to generate a logic plan for this selectStm.
	plan, err := MakePlan(stm, currentDB)
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
	storage.GetStorage().CreateSchema(stm.DatabaseName, string(stm.Charset), string(stm.Collate))
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

func columnTypeToFieldType(col parser.ColumnType) (ret storage.FieldTP) {
	// Todo: support column range.
	switch col.Tp {
	case parser.BOOL:
		ret.Name = storage.Bool
		return
	case parser.INT, parser.BIGINT:
		// For int, the range only affect display. see:
		// https://stackoverflow.com/questions/5634104/what-is-the-size-of-column-of-int11-in-mysql-in-bytes
		// for more detail.
		ret.Name = storage.Int
		return
	case parser.FLOAT:
		// For float, can refer here:
		// https://stackoverflow.com/questions/7979912/difference-between-float2-2-and-float-in-mysql
		ret.Name = storage.Float
		ret.Range = col.Ranges
		return
	case parser.CHAR:
		// For char and varchar, the size:
		// https://dev.mysql.com/doc/refman/8.0/en/char.html
		ret.Name = storage.Char
		ret.Range = col.Ranges
		return
	case parser.VARCHAR:
		ret.Name = storage.VarChar
		ret.Range = col.Ranges
		return
	case parser.TEXT:
		ret.Name = storage.Text
		return
	case parser.MEDIUMTEXT:
		ret.Name = storage.MediumText
		return
	case parser.BLOB:
		ret.Name = storage.Blob
		return
	case parser.MEDIUMBLOB:
		ret.Name = storage.MediumBlob
		return
	case parser.DATETIME:
		ret.Name = storage.DateTime
		return
	case parser.DATE:
		ret.Name = storage.Date
		return
	case parser.TIME:
		ret.Name = storage.Time
		return
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
		return errors.New(fmt.Sprintf("cannot find db: '%s'", schemaName))
	}
	tableSchema, err := getSchema(stm, dbInfo)
	if err != nil {
		return err
	}
	table := &storage.TableInfo{
		TableSchema: tableSchema,
		Charset:     string(stm.Charset),
		Collate:     string(stm.Collate),
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
			return errors.New(fmt.Sprintf("cannot found such table: %s", util.BuildDotString(schemaName, tableName)))
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
	if !storage.GetStorage().HasSchema(schemaName) {
		return errors.New(fmt.Sprintf("schema '%s' desn't find", schemaName))
	}
	if !storage.GetStorage().HasTable(schemaName, tableName) {
		return errors.New(fmt.Sprintf("table '%s.%s' doesn't find", schemaName, tableName))
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

func ExecuteRenameStm(stm *parser.RenameStm, currentDB string) error {
	for i, originTableName := range stm.OrigNames {
		schemaName, tableName, err := getSchemaTableName(originTableName, currentDB)
		if err != nil {
			return err
		}
		if !storage.GetStorage().HasSchema(schemaName) {
			return errors.New(fmt.Sprintf("schema '%s' doesn't find", schemaName))
		}
		if !storage.GetStorage().HasTable(schemaName, tableName) {
			return errors.New(fmt.Sprintf("table '%s.%s' doesn't find", schemaName, tableName))
		}
		newSchemaName, newTableName, err := getSchemaTableName(stm.ModifiedNames[i], currentDB)
		if err != nil {
			return err
		}
		if !storage.GetStorage().HasSchema(newSchemaName) {
			return errors.New(fmt.Sprintf("schema '%s' desn't find", newSchemaName))
		}
		if storage.GetStorage().HasTable(newSchemaName, newTableName) {
			return errors.New(fmt.Sprintf("table '%s.%s' already exist", newSchemaName, newTableName))
		}
		err = storage.GetStorage().GetDbInfo(schemaName).GetTable(tableName).RenameTo(newSchemaName, newTableName)
		if err != nil {
			return err
		}
	}
	return nil
}
