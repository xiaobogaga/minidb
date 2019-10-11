package storage

type StorageEngine struct {
	Dbs map[string]DbInfo
}

type DbInfo struct {
	Name   string
	Tables map[string]TableInfo
}

type TableInfo struct {
	Name string
	Rows []Row
}

type Row struct {
	Cols []interface{}
}

func NewStorageEngine() *StorageEngine {
	return &StorageEngine{Dbs: make(map[string]DbInfo)}
}

func (engine *StorageEngine) AddTable() {

}

func (engine *StorageEngine) GetDatabase() {

}

func (engine *StorageEngine) GetTable() {

}
