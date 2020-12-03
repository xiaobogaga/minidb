package storage

import "errors"

type Storage struct {
	Dbs map[string]DbInfo
}

func (storage Storage) HasSchema(schema string) bool {
	_, ok := storage.Dbs[schema]
	return ok
}

func (storage Storage) GetDbInfo(schema string) DbInfo {
	return storage.Dbs[schema]
}

var storage Storage = Storage{}

func GetStorage() Storage {
	return storage
}

type DbInfo struct {
	Name   string
	Tables map[string]TableInfo
}

func (dbs DbInfo) HasTable(table string) bool {
	_, ok := dbs.Tables[table]
	return ok
}

func (dbs DbInfo) GetTable(table string) TableInfo {
	return dbs.Tables[table]
}

type TableInfo struct {
	Schema Schema
}

type Schema struct {
	FieldMap map[string]map[string]map[string]Field // a schema can be a join of two tables: map[schema]map[tableName][colName]Field
}

// FetchData returns the data starting at row index `rowIndex` and the batchSize is batchSize.
func (table TableInfo) FetchData(rowIndex, batchSize int) RecordBatch {
	// Todo
}

func (schema Schema) HasSubTable(tableName string) bool {
	_, ok := schema.FieldMap[tableName]
	return ok
}

// HasColumn returns whether this schema has such schema, table and column.
// schemaName, tableName can be empty, then it will iterate all dbschema to find such column.
func (schema Schema) HasColumn(schemaName, tableName, columnName string) bool {
	if schemaName != "" {
		dbSchema, ok := schema.FieldMap[schemaName]
		if !ok {
			return false
		}
		tableSchema, ok := dbSchema[tableName]
		if !ok {
			return false
		}
		_, ok = tableSchema[columnName]
		return ok
	}
	if tableName != "" {
		for _, dbSchema := range schema.FieldMap {
			tableSchema, ok := dbSchema[tableName]
			if !ok {
				continue
			}
			_, ok = tableSchema[columnName]
			return ok
		}
		return false
	}
	for _, dbSchema := range schema.FieldMap {
		for _, tableSchema := range dbSchema {
			for colName, _ := range tableSchema {
				if colName == columnName {
					return true
				}
			}
		}
	}
	return false
}

func (schema Schema) HasAmbiguousColumn(schemaName, tableName, columnName string) bool {
	if schemaName != "" && tableName != "" {
		return false
	}
	i := 0
	if tableName == "" {
		for _, dbSchema := range schema.FieldMap {
			for _, tableSchema := range dbSchema {
				for colName, _ := range tableSchema {
					if colName == columnName {
						i++
					}
				}
			}
		}
		return i >= 2
	}
	i = 0
	// Then tableName mustn't be empty and schemaName must be empty.
	for _, dbSchema := range schema.FieldMap {
		tableSchema, ok := dbSchema[tableName]
		if !ok {
			continue
		}
		for colName, _ := range tableSchema {
			if colName == columnName {
				i++
			}
		}
	}
	return i >= 2
}

func (schema Schema) GetField(columnName string) Field {
	for _, dbSchema := range schema.FieldMap {
		for _, tableSchema := range dbSchema {
			for colName, f := range tableSchema {
				if colName == columnName {
					return f
				}
			}
		}
	}
	// Todo
	return Field{}
}

func (schema Schema) Merge(right Schema) (Schema, error) {
	ret := Schema{
		FieldMap: map[string]map[string]map[string]Field{},
	}
	for dbSchemaName, dbSchema := range schema.FieldMap {
		ret.AddNewDbTableSchema(dbSchemaName, dbSchema)
	}
	// Merge right schema. We doesn't allow to merge same table(means belongs to same database)
	for dbSchemaName, dbSchema := range right.FieldMap {
		retDbSchema, ok := ret.FieldMap[dbSchemaName]
		if !ok {
			ret.AddNewDbTableSchema(dbSchemaName, dbSchema)
			continue
		}
		for tableName, tableSchema := range dbSchema {
			retTableSchema, ok := retDbSchema[tableName]
			if ok {
				return Schema{}, errors.New("duplicate table name")
			}
			retTableSchema = map[string]Field{}
			for col, field := range tableSchema {
				retTableSchema[col] = field
			}
			retDbSchema[tableName] = retTableSchema
		}
	}
	return ret, nil
}

func (schema Schema) AddNewDbTableSchema(dbName string, dbSchema map[string]map[string]Field) {
	retDbSchema := map[string]map[string]Field{}
	for tableName, table := range dbSchema {
		retTableSchema := map[string]Field{}
		for col, field := range table {
			retTableSchema[col] = field
		}
		retDbSchema[tableName] = retTableSchema
	}
	schema.FieldMap[dbName] = retDbSchema
}

func (schema Schema) AddField() {

}

type RecordBatch struct {
	Fields  map[string]Field
	Records map[string]ColumnVector
}

type Field struct {
	TP         FieldTP
	Name       string
	TableName  string
	SchemaName string
}

// A column of field.
type ColumnVector struct {
	Field  Field
	Values []interface{}
}

func (column ColumnVector) GetField() Field {
	return column.Field
}

func (column ColumnVector) GetTP() FieldTP {
	return column.Field.TP
}

func (column ColumnVector) Size() int {
	return len(column.Values)
}

type FieldTP string

const (
	Identifier FieldTP = "identifier"
	Bool       FieldTP = "bool"
	Int        FieldTP = "int"
	BigInt     FieldTP = "bigint"
	Float      FieldTP = "float"
	Char       FieldTP = "char"
	VarChar    FieldTP = "varchar"
	DateTime   FieldTP = "datetime"
	Blob       FieldTP = "blob"
	MediumBlob FieldTP = "mediumBlob"
	Text       FieldTP = "text"
	MediumText FieldTP = "mediumText"
	Constant   FieldTP = "constant" // like numeric or string or char value.
)

func IsFieldNumerialType(f Field) bool {
	return f.TP == Int || f.TP == BigInt || f.TP == Float
}
