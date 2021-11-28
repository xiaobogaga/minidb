package storage

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/xiaobogaga/minidb/util"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Storage struct {
	Dbs map[string]*DbInfo
}

func (storage *Storage) HasSchema(schema string) bool {
	_, ok := storage.Dbs[schema]
	return ok
}

func (storage *Storage) GetDbInfo(schema string) *DbInfo {
	return storage.Dbs[schema]
}

func (storage *Storage) CreateSchema(name, charset, collate string) {
	schema := &DbInfo{Name: name, Charset: charset, Collate: collate, Tables: map[string]*TableInfo{}}
	storage.Dbs[schema.Name] = schema
}

func (storage *Storage) RemoveSchema(schema string) {
	delete(storage.Dbs, schema)
}

func (storage *Storage) HasTable(schema, table string) bool {
	if !storage.HasSchema(schema) {
		return false
	}
	db := storage.GetDbInfo(schema)
	return db.HasTable(table)
}

var storage = &Storage{Dbs: map[string]*DbInfo{}}

func GetStorage() *Storage {
	return storage
}

type DbInfo struct {
	Name    string
	Charset string
	Collate string
	Tables  map[string]*TableInfo
}

func (dbs *DbInfo) HasTable(table string) bool {
	_, ok := dbs.Tables[table]
	return ok
}

func (dbs *DbInfo) GetTable(table string) *TableInfo {
	return dbs.Tables[table]
}

func (dbs *DbInfo) AddTable(table *TableInfo) {
	dbs.Tables[table.TableSchema.TableName()] = table
}

func (dbs *DbInfo) RemoveTable(tableName string) {
	delete(dbs.Tables, tableName)
}

type TableInfo struct {
	TableSchema *TableSchema
	Charset     string
	Collate     string
	Engine      string
	Datas       []*ColumnVector
}

func createRecordBatchFromColumns(columns []Field) *RecordBatch {
	ret := &RecordBatch{
		Fields:  columns,
		Records: make([]*ColumnVector, len(columns)),
	}
	for i, column := range ret.Fields {
		ret.Records[i] = &ColumnVector{Field: column}
	}
	return ret
}

// FetchData returns the data starting at row index `rowIndex` And the batchSize Is batchSize.
func (table *TableInfo) FetchData(rowIndex, batchSize int) *RecordBatch {
	if len(table.Datas) == 0 || rowIndex >= table.Datas[1].Size() {
		return nil
	}
	ret := createRecordBatchFromColumns(table.TableSchema.Columns)
	for i := rowIndex; (i-rowIndex) < batchSize && i < table.Datas[1].Size(); i++ {
		table.FillRowInfo(ret, i)
	}
	return ret
}

func (table *TableInfo) FillRowInfo(ret *RecordBatch, row int) {
	for j, col := range table.Datas {
		// The first row is the row index.
		if j == 0 {
			ret.Records[j].Append(EncodeInt(int64(row)))
		} else {
			ret.Records[j].Append(col.Values[row])
		}
	}
}

// Return the column index in table and it's field.
func (table *TableInfo) GetColumnInfo(column string) (int, *Field) {
	for i, col := range table.Datas {
		if col.Field.Name == column {
			return i, &col.Field
		}
	}
	return -1, nil
}

func (table *TableInfo) HasColumn(column string) bool {
	for _, col := range table.Datas {
		if col.Field.Name == column {
			return true
		}
	}
	return false
}

const (
	// DefaultPrimaryKeyName = "0_id"
	DefaultRowKeyName = ""
)

//func DefaultPrimaryKeyColumn(schemaName, tableName string) Field {
//	return Field{SchemaName: schemaName, TableName: tableName, Name: DefaultPrimaryKeyName, TP: Int, PrimaryKey: true, AutoIncrement: true, AllowNull: false}
//}

// update tableInfo col to new value `value` at row index row.
func (table *TableInfo) UpdateData(colName string, row int, value []byte) error {
	index, col := table.GetColumnInfo(colName)
	err := col.CanAssign(value)
	if err != nil {
		return err
	}
	table.Datas[index].Values[row] = value
	return nil
}

func (table *TableInfo) DeleteRow(row int) {
	for i := 1; i < len(table.Datas); i++ {
		table.Datas[i].Values = append(table.Datas[i].Values[:row], table.Datas[i].Values[row+1:]...)
	}
}

func (table *TableInfo) Truncate() {
	for i := 0; i < len(table.Datas); i++ {
		table.Datas[i].Values = nil
	}
}

func (table *TableInfo) InsertData(cols []string, values [][]byte) {
	for i, col := range cols {
		for _, tableCol := range table.Datas {
			if tableCol.Field.Name == col {
				tableCol.Append(values[i])
				break
			}
		}
	}
}

func (table *TableInfo) Describe() *RecordBatch {
	ret := &RecordBatch{
		Fields: []Field{
			{Name: "Properties", TP: FieldTP{Name: Text}},
			{Name: "Values", TP: FieldTP{Name: Text}},
		},
		Records: make([]*ColumnVector, 2),
	}
	ret.Records[0] = &ColumnVector{Field: ret.Fields[0]}
	ret.Records[1] = &ColumnVector{Field: ret.Fields[1]}
	ret.Records[0].Append([]byte("database"))
	ret.Records[1].Append([]byte(table.TableSchema.SchemaName()))
	ret.Records[0].Append([]byte("table"))
	ret.Records[1].Append([]byte(table.TableSchema.TableName()))
	for i := 1; i < len(table.TableSchema.Columns); i++ {
		col := table.TableSchema.Columns[i]
		if col.Name == DefaultRowKeyName {
			continue
		}
		ret.Records[0].Append([]byte(fmt.Sprintf("col: %s", col.Name)))
		ret.Records[1].Append([]byte(fmt.Sprintf("%s(%d, %d), [%v, %v, %v], %s", col.TP.Name, col.TP.Range[0], col.TP.Range[1],
			col.PrimaryKey, col.AutoIncrement, col.AllowNull, DecodeToString(col.DefaultValue, col.TP))))
	}
	return ret
}

func (table *TableInfo) RenameTo(newSchemaName string, newTableName string) error {
	// First we remove the table from old schema first.
	storage.GetDbInfo(table.TableSchema.SchemaName()).RemoveTable(table.TableSchema.TableName())
	// Now change table info to new db and new table name.
	table.TableSchema.SetSchemaTableName(newSchemaName, newTableName)
	for _, col := range table.Datas {
		if col == nil {
			continue
		}
		col.Field.TableName = newTableName
		col.Field.SchemaName = newSchemaName
	}
	storage.GetDbInfo(table.TableSchema.SchemaName()).AddTable(table)
	return nil
}

// A table format looks like this.
// | rowIndex | cols ... | DefaultPrimaryKey (if cols doesn't have primary key column |
// the rowIndex column has no content by default. But when the fetch data is called.
// We will feed the row index value to the rowIndex column.

// A SingleTableSchema is a list of Fields representing a temporal table format.
// It can has multiple columns, each column has a DatabaseName, TableRef, ColumnName to allow
// multiple columns coexist with same columnName but are from different database.
type TableSchema struct {
	Columns []Field
}

func (schema *TableSchema) AppendColumn(field Field) {
	schema.Columns = append(schema.Columns, field)
}

func (schema *TableSchema) TableName() string {
	return schema.Columns[0].TableName
}

func (schema *TableSchema) SchemaName() string {
	return schema.Columns[0].SchemaName
}

func isSameColumn(schemaName, tableName, columnName string, expected Field) bool {
	return expected.Name == columnName && (schemaName == "" || schemaName == expected.SchemaName) &&
		(tableName == "" || tableName == expected.TableName)
}

func (schema *TableSchema) GetTableInfoFromColumn(schemaName, tableName, columnName string) (*TableInfo, error) {
	var col Field
	for _, column := range schema.Columns {
		if isSameColumn(schemaName, tableName, columnName, column) {
			col = column
			break
		}
	}
	schemaName = col.SchemaName
	tableName = col.TableName
	dbInfo := storage.GetDbInfo(schemaName)
	if dbInfo == nil {
		return nil, nil
	}
	return dbInfo.GetTable(tableName), nil
}

// HasColumn returns whether this schema has such schema, table And column.
// schemaName, tableName can be empty, then it will iterate all db schema to find such column.
func (schema *TableSchema) HasColumn(schemaName, tableName, columnName string) bool {
	for _, column := range schema.Columns {
		if isSameColumn(schemaName, tableName, columnName, column) {
			return true
		}
	}
	return false
}

func (schema *TableSchema) HasAmbiguousColumn(schemaName, tableName, columnName string) bool {
	if schemaName != "" && tableName != "" {
		return false
	}
	times := 0
	for _, column := range schema.Columns {
		if isSameColumn(schemaName, tableName, columnName, column) {
			times++
		}
	}
	return times > 1
}

func (schema *TableSchema) GetField(databaseName string, tableName string, columnName string) *Field {
	for _, column := range schema.Columns {
		if isSameColumn(databaseName, tableName, columnName, column) {
			return &column
		}
	}
	return nil
}

func (schema *TableSchema) Merge(right *TableSchema) (*TableSchema, error) {
	ret := &TableSchema{} // Are we safe here.
	ret.Columns = append(ret.Columns, schema.Columns...)
	ret.Columns = append(ret.Columns, right.Columns...)
	return ret, nil
}

func (schema *TableSchema) SetSchemaTableName(schemaName string, tableName string) {
	for i := range schema.Columns {
		schema.Columns[i].SchemaName, schema.Columns[i].TableName = schemaName, tableName
	}
}

type RecordBatch struct {
	Fields  []Field         `json:"fields"`
	Records []*ColumnVector `json:"records"`
}

func (recordBatch *RecordBatch) RowCount() int {
	if recordBatch == nil {
		return 0
	}
	return recordBatch.Records[0].Size()
}

func (recordBatch *RecordBatch) GetColumnValue(schemaName, tableName, colName string) *ColumnVector {
	for _, col := range recordBatch.Records {
		if isSameColumn(schemaName, tableName, colName, col.Field) {
			return col
		}
	}
	return nil
}

func (recordBatch *RecordBatch) ColumnCount() int {
	return len(recordBatch.Records)
}

type JoinType byte

const (
	LeftJoin JoinType = iota
	RightJoin
	InnerJoin
)

func createRecordBatchFromTableSchema(schema *TableSchema, size int) *RecordBatch {
	ret := &RecordBatch{
		Fields:  make([]Field, len(schema.Columns)),
		Records: make([]*ColumnVector, len(schema.Columns)),
	}
	for i := 0; i < len(schema.Columns); i++ {
		f := schema.Columns[i]
		ret.Fields[i] = f
		// ret.Fields[i].Name = fmt.Sprintf("%s.%s", f.TableName, f.Name)
		ret.Records[i] = &ColumnVector{Field: ret.Fields[i], Values: make([][]byte, size)}
	}
	return ret
}

func maxSizeOf(left *RecordBatch, right *RecordBatch) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return len(right.Records[1].Values)
	}
	if right == nil {
		return len(left.Records[1].Values)
	}
	return len(left.Records[1].Values) * len(right.Records[1].Values)
}

// recordBatch join another.
func (recordBatch *RecordBatch) Join(another *RecordBatch, leftSchema *TableSchema, finalSchema *TableSchema) *RecordBatch {
	if recordBatch == nil && another == nil {
		return nil
	}
	size := maxSizeOf(recordBatch, another)
	// First prepare ret from finalSchema first.
	ret := createRecordBatchFromTableSchema(finalSchema, size)
	if recordBatch == nil || another == nil {
		JoinWithNull(ret, recordBatch, another, len(leftSchema.Columns))
		return ret
	}
	// Now we join left and right. they are not null.
	// recordBatch: 3 rows, another: 2 rows
	for i := 0; i < size; i++ {
		rowRight := i % another.RowCount()
		rowLeft := i / another.RowCount()
		for j := 0; j < recordBatch.ColumnCount(); j++ {
			ret.Records[j].Set(i, recordBatch.Records[j].RawValue(rowLeft))
		}
		for j := 0; j < another.ColumnCount(); j++ {
			col := j + recordBatch.ColumnCount()
			ret.Records[col].Set(i, another.Records[j].RawValue(rowRight))
		}
	}
	return ret
}

// Join left, right to ret. and one of left, right is null.
func JoinWithNull(ret *RecordBatch, left, right *RecordBatch, j int) {
	// set column vector.
	if left != nil {
		for i, col := range left.Records {
			ret.Records[i].Values = col.Values
		}
	}
	if right != nil {
		for i, col := range right.Records {
			ret.Records[i+j].Values = col.Values
		}
	}
}

// Append new to recordBatch, they are in the same layout.
func (recordBatch *RecordBatch) Append(new *RecordBatch) {
	for i, col := range new.Records {
		recordBatch.Records[i].Appends(col)
	}
}

// columnVector represents the order of recordBatch. It's has just one row.
// whose field Is Field{Name: "order", TP: storage.Int}.
func (recordBatch *RecordBatch) OrderBy(columnVector *ColumnVector) {
	temp := &RecordBatch{Fields: recordBatch.Fields, Records: make([]*ColumnVector, len(recordBatch.Records))}
	for i, col := range recordBatch.Records {
		temp.Records[i] = &ColumnVector{
			Field:  temp.Fields[i],
			Values: make([][]byte, len(col.Values)),
		}
	}
	// Reorder
	for j := 0; j < columnVector.Size(); j++ {
		// Move j -> oldIndex
		oldIndex := columnVector.Int(j)
		for i, col := range recordBatch.Records {
			temp.Records[i].Values[j] = col.Values[oldIndex]
		}
	}
	recordBatch.Copy(temp, 0, 0, temp.RowCount())
}

// Set the i-th column values in recordBatch by using columnVector.
func (recordBatch *RecordBatch) SetColumnValue(col int, columnVector *ColumnVector) {
	recordBatch.Records[col] = columnVector
}

// Append row i of record to recordBatch.
func (recordBatch *RecordBatch) AppendRecord(record *RecordBatch, row int) {
	for col := 0; col < recordBatch.ColumnCount(); col++ {
		recordBatch.Records[col].Append(record.Records[col].Values[row])
	}
}

// Copy src[srcFrom:srcFrom + size] to recordBatch[descFrom:], recordBatch must have enough memory to save all data.
func (recordBatch *RecordBatch) Copy(src *RecordBatch, srcFrom, descFrom, size int) {
	for i := srcFrom; i < srcFrom+size && i < src.RowCount(); i++ {
		// Copy one row.
		for j := 0; j < src.ColumnCount(); j++ {
			recordBatch.Records[j].Values[descFrom] = src.Records[j].Values[i]
		}
		descFrom++
	}
}

// selectedRows Is a bool column which represent each row in recordBatch Is selected Or not.
func (recordBatch *RecordBatch) Filter(selectedRows *ColumnVector) *RecordBatch {
	ret := MakeEmptyRecordBatchFrom(recordBatch)
	// now we copy the selected rows.
	for i := 0; i < recordBatch.RowCount(); i++ {
		if !selectedRows.Bool(i) {
			continue
		}
		for j := 0; j < recordBatch.ColumnCount(); j++ {
			ret.Records[j].Append(recordBatch.Records[j].Values[i])
		}
	}
	return ret
}

// Return data[startIndex: startIndex + size - 1]
func (recordBatch *RecordBatch) Slice(startIndex, size int) *RecordBatch {
	if startIndex >= recordBatch.RowCount() {
		return nil
	}
	ret := MakeEmptyRecordBatchFrom(recordBatch)
	for i := startIndex; i < startIndex+size && i < recordBatch.RowCount(); i++ {
		// Copy one row.
		for j := 0; j < recordBatch.ColumnCount(); j++ {
			ret.Records[j].Append(recordBatch.Records[j].Values[i])
		}
	}
	return ret
}

func MakeEmptyRecordBatchFrom(src *RecordBatch) *RecordBatch {
	ret := &RecordBatch{
		Fields:  make([]Field, src.ColumnCount()),
		Records: make([]*ColumnVector, src.ColumnCount()),
	}
	// copy field And column vector field first.
	for i, f := range src.Fields {
		ret.Fields[i] = f
		ret.Records[i] = &ColumnVector{Field: f}
	}
	return ret
}

// Encode row key.
func (recordBatch *RecordBatch) RowKey(row int) (key []byte) {
	if row >= recordBatch.RowCount() {
		return
	}
	for i := 0; i < recordBatch.ColumnCount(); i++ {
		key = append(key, EncodeInt(int64(len(recordBatch.Records[i].Values[row])))...) // 8 byte length.
		key = append(key, recordBatch.Records[i].Values[row]...)
	}
	return
}

// Return the rowIndex in the row-th data.
func (recordBatch *RecordBatch) RowIndex(tableName string, row int) (int, error) {
	for i := 0; i < recordBatch.ColumnCount(); i++ {
		if recordBatch.Fields[i].TableName == tableName {
			return int(DecodeInt(recordBatch.Records[i].Values[row])), nil
		}
	}
	return 0, errors.New("unable found such table")
}

func (recordBatch *RecordBatch) IsRowIdColumn(col int) bool {
	return col < recordBatch.ColumnCount() && recordBatch.Fields[col].Name == DefaultRowKeyName
}

// For type check.
type Field struct {
	TP            FieldTP
	Name          string
	Alias         string
	TableName     string
	SchemaName    string
	DefaultValue  []byte
	AllowNull     bool
	AutoIncrement bool
	PrimaryKey    bool
}

func (f Field) IsString() bool {
	return f.TP.Name == Char || f.TP.Name == VarChar || f.TP.Name == Text || f.TP.Name == MediumText ||
		f.TP.Name == DateTime || f.TP.Name == Date || f.TP.Name == Time || f.TP.Name == Blob || f.TP.Name == MediumBlob
}

func (f Field) IsNumerical() bool {
	return f.TP.Name == Int || f.TP.Name == Float
}

func (f Field) IsBool() bool {
	return f.TP.Name == Bool
}

func (f Field) IsInteger() bool {
	return f.TP.Name == Int
}

func (f Field) IsFloat() bool {
	return f.TP.Name == Float
}

func (f Field) IsMultiple() bool {
	return f.TP.Name == Multiple
}

func (f Field) CanOp(another Field, opType OpType) (err error) {
	switch opType {
	case NegativeOpType:
		if !f.IsNumerical() {
			err = errors.New("- cannot apply to non numerical type")
		}
		return
	case AddOpType, MinusOpType, MulOpType, DivideOpType:
		if f.IsNumerical() && another.IsNumerical() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s cannot apply to non numerical type", opType))
	case ModOpType:
		if f.IsInteger() && another.IsInteger() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s cannot apply to non integer type", opType))
	case AndOpType, OrOpType:
		if f.IsBool() && another.IsBool() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s cannot apply to non bool type", opType))
	case EqualOpType, NotEqualOpType, IsOpType:
		if f.IsNumerical() && another.IsNumerical() {
			return nil
		}
		if f.IsString() && another.IsString() {
			return nil
		}
		if f.IsBool() && another.IsBool() {
			return nil
		}
		return errors.New(fmt.Sprintf("type doesn't match on %s", opType))
	case LessOpType, LessEqualOpType, GreatEqualOpType, GreatOpType:
		if f.IsNumerical() && another.IsNumerical() {
			return nil
		}
		if f.IsString() && another.IsString() {
			return nil
		}
		if f.IsBool() && another.IsBool() {
			return nil
		}
		return errors.New(fmt.Sprintf("type doesn't match on %s", opType))
	default:
		panic("wrong opType")
	}
}

var (
	dateTimeLayout = "2006-01-02 15:04:05"
	dateLayout     = "2006-01-02"
	timeLayout     = "15:04:05"
)

// Check whether we can assign val to this field.
// Will do several checking:
// * length check for varchar and char.
// * datetime format check.
func (f Field) CanAssign(val []byte) (err error) {
	switch f.TP.Name {
	case Char, VarChar:
		if len(val) > f.TP.Range[0] {
			err = errors.New("data too long")
		}
	case DateTime:
		_, err = time.Parse(dateTimeLayout, string(val))
		if err != nil {
			err = errors.New("wrong datetime format")
		}
	case Date:
		_, err = time.Parse(dateLayout, string(val))
		if err != nil {
			err = errors.New("wrong date format")
		}
	case Time:
		_, err = time.Parse(timeLayout, string(val))
		if err != nil {
			err = errors.New("wrong time format")
		}
	case Multiple:
		err = errors.New("cannot assign to * type")
	}
	return
}

//func (f Field) Cascade(val []byte) []byte {
//	if f.TP.Name != Float {
//		return val
//	}
//	digits, decimals := f.TP.Range[0], f.TP.Range[1]
//	value := fmt.Sprintf(fmt.Sprintf("%s%d.%df", "%", digits, decimals), DecodeFloat(val))
//	v, _ := strconv.ParseFloat(value, 64)
//	return EncodeFloat(v)
//}

func (f Field) CanIgnoreInInsert() bool {
	return f.Name == DefaultRowKeyName || f.AllowNull
}

func (f Field) ColumnName() (name string) {
	if f.SchemaName != "" {
		name = f.SchemaName
	}
	if f.TableName != "" {
		name = fmt.Sprintf("%s.%s", name, f.TableName)
	}
	if name != "" {
		name = fmt.Sprintf("%s.%s", name, f.Name)
	} else {
		name = f.Name
	}
	return name
}

func RowIndexField(schemaName, tableName string) Field {
	field := Field{
		SchemaName:    schemaName,
		TableName:     tableName,
		Name:          DefaultRowKeyName,
		TP:            FieldTP{Name: Int},
		AllowNull:     true,
		AutoIncrement: false,
		PrimaryKey:    false,
	}
	return field
}

type OpType byte

const (
	AddOpType OpType = iota
	MinusOpType
	MulOpType
	DivideOpType
	AndOpType
	OrOpType
	ModOpType
	GreatOpType
	GreatEqualOpType
	LessOpType
	LessEqualOpType
	EqualOpType
	NotEqualOpType
	IsOpType
	NegativeOpType
)

func (tp OpType) String() string {
	switch tp {
	case AddOpType:
		return "+"
	case MinusOpType:
		return "-"
	case MulOpType:
		return "*"
	case DivideOpType:
		return "/"
	case AndOpType:
		return "and"
	case OrOpType:
		return "or"
	case ModOpType:
		return "%"
	case GreatOpType:
		return ">"
	case GreatEqualOpType:
		return ">="
	case LessOpType:
		return "<"
	case LessEqualOpType:
		return "<="
	case EqualOpType:
		return "="
	case NotEqualOpType:
		return "!="
	case IsOpType:
		return "Is"
	case NegativeOpType:
		return "-"
	default:
		panic("unknown op")
	}
}

func (tp OpType) Comparator() bool {
	return tp == IsOpType || tp == EqualOpType || tp == NotEqualOpType || tp == GreatOpType ||
		tp == GreatEqualOpType || tp == LessOpType || tp == LessEqualOpType
}

func (tp OpType) Logic() bool {
	return tp == AndOpType || tp == OrOpType
}

var typeOpMap = map[string]FieldTPName{
	"int + int":      Int,
	"int + float":    Float,
	"float + int":    Float,
	"float + float":  Float,
	"int - int":      Int,
	"int - float":    Float,
	"float - int":    Float,
	"float - float":  Float,
	"int * int":      Int,
	"int * float":    Float,
	"float * int":    Float,
	"float * float":  Float,
	"int / int":      Int,
	"int / float":    Float,
	"float / int":    Float,
	"float / float":  Float,
	"int % int":      Int,
	"int = int":      Bool,
	"int = float":    Bool,
	"float = int":    Bool,
	"float = float":  Bool,
	"int Is int":     Bool,
	"int Is float":   Bool,
	"float Is int":   Bool,
	"float Is float": Bool,
	"int != int":     Bool,
}

// Return the new type after we apply f op another.
// Didn't do type match check here and assume user already did type checking.
func (f Field) InferenceType(another Field, op OpType) FieldTP {
	if op.Comparator() {
		return DefaultFieldTpMap[Bool]
	}
	if op.Logic() {
		return DefaultFieldTpMap[Bool]
	}
	key := fmt.Sprintf("%s %s %s", f.TP.Name, op, another.TP.Name)
	fieldTpName := typeOpMap[key]
	ret := FieldTP{Name: fieldTpName}
	// Return the maximum range of these two types.
	if ret.Name == Float {
		ret.Range[0] = util.Max(f.TP.Range[0], another.TP.Range[0])
		ret.Range[1] = util.Max(f.TP.Range[1], another.TP.Range[1])
	}
	return ret
}

func InferenceType(data []byte) FieldTP {
	if strings.ToUpper(string(data)) == "TRUE" || strings.ToUpper(string(data)) == "FALSE" {
		return FieldTP{Name: Bool}
	}
	if data[0] >= '0' && data[0] <= '9' {
		return InferenceNumericalType(data)
	}
	if data[0] == '\'' || data[0] == '"' {
		return FieldTP{Name: Text}
	}
	panic("unknown data type")
}

// Will use maximum ranges.
func InferenceNumericalType(data []byte) FieldTP {
	if bytes.IndexByte(data, '.') == -1 {
		return DefaultFieldTpMap[Int]
	}
	return DefaultFieldTpMap[Float]
}

// A column of field.
type ColumnVector struct {
	Field  Field
	Values [][]byte
}

func (column *ColumnVector) GetField() Field {
	return column.Field
}

func (column *ColumnVector) GetTP() FieldTP {
	return column.Field.TP
}

func (column *ColumnVector) Size() int {
	return len(column.Values)
}

func (column *ColumnVector) RawValue(row int) []byte {
	return column.Values[row]
}

func (column *ColumnVector) Negative() *ColumnVector {
	// column must be a numeric type
	ret := &ColumnVector{Field: column.Field}
	for _, value := range column.Values {
		v := Negative(column.Field.TP, value)
		ret.Values = append(ret.Values, v)
	}
	return ret
}

// Add another And column And return the new column with name `name`
func (column *ColumnVector) Add(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{TP: column.Field.InferenceType(another.Field, AddOpType), Name: name},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Add(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

// Minus another And column And return the new column with name `name`
func (column *ColumnVector) Minus(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{TP: column.Field.InferenceType(another.Field, MinusOpType), Name: name},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Minus(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Mul(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{TP: column.Field.InferenceType(another.Field, MulOpType), Name: name},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Mul(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Divide(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{TP: column.Field.InferenceType(another.Field, DivideOpType), Name: name},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Divide(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Mod(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{TP: column.Field.InferenceType(another.Field, ModOpType), Name: name},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Mod(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Equal(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Equal(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Is(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Is(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) NotEqual(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(NotEqual(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Great(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Great(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) GreatEqual(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(GreatEqual(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) Less(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Less(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) LessEqual(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(LessEqual(val1, column.Field.TP, val2, another.Field.TP))
	}
	return ret
}

func (column *ColumnVector) And(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(And(val1, val2))
	}
	return ret
}

func (column *ColumnVector) Or(another *ColumnVector, name string) *ColumnVector {
	ret := &ColumnVector{
		Field: Field{Name: name, TP: DefaultFieldTpMap[Bool]},
	}
	for i := 0; i < column.Size(); i++ {
		val1 := column.RawValue(i)
		val2 := another.RawValue(i)
		ret.Append(Or(val1, val2))
	}
	return ret
}

type sortTrick struct {
	RetValue   []byte
	SortValues [][]byte
}

// sort column by using order specified by others in order asc.
func (column *ColumnVector) Sort(others []*ColumnVector, asc []bool) *ColumnVector {
	ret := &ColumnVector{Field: column.Field}
	// This must be careful to make sure columns in others are swapped along with ret.values.
	sortTrick := make([]sortTrick, column.Size())
	for i := 0; i < column.Size(); i++ {
		sortTrick[i].RetValue = column.RawValue(i)
		sortTrick[i].SortValues = make([][]byte, len(others))
		for j := 0; j < len(others); j++ {
			sortTrick[i].SortValues[j] = others[j].RawValue(i)
		}
	}
	sort.Slice(sortTrick, func(i, j int) bool {
		for h := 0; h < len(others); h++ {
			sortColumn := others[h]
			c := compare(sortTrick[i].SortValues[h], sortColumn.GetTP(), sortTrick[j].SortValues[h], sortColumn.GetTP())
			if c == 0 {
				continue
			}
			if c < 0 {
				return asc[h]
			}
			if c > 0 {
				return !asc[h]
			}
		}
		return i < j
	})
	// copy sortTricks.values to ret.
	for i := 0; i < column.Size(); i++ {
		ret.Append(sortTrick[i].RetValue)
	}
	// ret.Print()
	return ret
}

// column must be a bool column
func (column *ColumnVector) Bool(row int) bool {
	return DecodeBool(column.Values[row])
}

// column must a integer column.
func (column *ColumnVector) Int(row int) int64 {
	return DecodeInt(column.Values[row])
}

func (column *ColumnVector) String(row int) string {
	return DecodeToString(column.RawValue(row), column.Field.TP)
}

func (column *ColumnVector) Float(row int) float64 {
	return DecodeFloat(column.RawValue(row))
}

func (column *ColumnVector) Append(value []byte) {
	column.Values = append(column.Values, value)
}

func (column *ColumnVector) Appends(another *ColumnVector) {
	column.Values = append(column.Values, another.Values...)
}

const NULL = "NULL"

func (column *ColumnVector) ToString(row int) string {
	if row >= len(column.Values) {
		return NULL
	}
	switch column.Field.TP.Name {
	case Text, Char, VarChar, MediumText, Blob, MediumBlob, DateTime, Date, Time:
		// we can compare them by bytes.
		return string(column.Values[row])
	case Bool:
		if column.Bool(row) {
			return "1"
		}
		return "0"
	case Int:
		return strconv.FormatInt(DecodeInt(column.Values[row]), 10)
	case Float:
		v := DecodeFloat(column.Values[row])
		return fmt.Sprintf(fmt.Sprintf("%s.%df", "%", column.Field.TP.Range[1]), v)
	default:
		panic("unknown type")
	}
}

func (column *ColumnVector) Print() {
	for i := 0; i < column.Size(); i++ {
		println(column.ToString(i))
	}
}

func (column *ColumnVector) Set(row int, data []byte) {
	column.Values[row] = data
}

type FieldTP struct {
	Name  FieldTPName
	Range [2]int
}

type FieldTPName string

const (
	Bool       FieldTPName = "bool"
	Int        FieldTPName = "int"
	Float      FieldTPName = "float"
	Char       FieldTPName = "char"
	VarChar    FieldTPName = "varchar"
	DateTime   FieldTPName = "datetime"
	Date       FieldTPName = "date"
	Time       FieldTPName = "time"
	Blob       FieldTPName = "blob"
	MediumBlob FieldTPName = "mediumBlob"
	Text       FieldTPName = "text"
	MediumText FieldTPName = "mediumText"
	Multiple   FieldTPName = "*"
)

// Several no range fieldTP map.
var DefaultFieldTpMap = map[FieldTPName]FieldTP{
	Bool:       {Name: Bool},
	DateTime:   {Name: DateTime},
	Date:       {Name: Date},
	Time:       {Name: Time},
	Blob:       {Name: Blob},
	MediumBlob: {Name: MediumBlob},
	Text:       {Name: Text},
	MediumText: {Name: MediumText},
	Int:        {Name: Int},
	Float:      {Name: Float, Range: [2]int{64, 64}},
	Char:       {Name: Float, Range: [2]int{1 << 8}},
	VarChar:    {Name: Float, Range: [2]int{1 << 16}},
	Multiple:   {Name: Multiple},
}
