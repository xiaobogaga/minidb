package storage

type Storage struct {
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
