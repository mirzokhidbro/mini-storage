package storage

import (
	"errors"
)

type ColumnType int

const (
	TypeInt ColumnType = iota
	TypeVarchar
)

type Column struct {
	Name   string
	Type   ColumnType
	Length int
}

type Schema struct {
	Columns []Column
}

type Record struct {
	Items []Item
}

type Item struct {
	Literal interface{}
}

type TableManager struct {
	FileManager *FileManager
}

func NewTableManager(filePath string) (*TableManager, error) {
	file_manager, err := NewFileManager(filePath)
	if err != nil {
		panic(err.Error())
	}
	return &TableManager{FileManager: file_manager}, nil
}

func (tm *TableManager) CreateTable(name string, schema *Schema) error {
	schema_exist, err := tm.FileManager.FileExists(name + ".schema")

	if err != nil {
		return err
	}

	if schema_exist {
		return errors.New("file already exists")
	}

	table_exist, err := tm.FileManager.FileExists(name + ".table")

	if err != nil {
		return err
	}

	if table_exist {
		return errors.New("table already exists")
	}

	schema_file, err := tm.FileManager.CreateFile(name + ".schema")

	if err != nil {
		return err
	}

	_, err = tm.FileManager.CreateFile(name + ".table")

	if err != nil {
		return err
	}

	serialized_schema := SerializeSchema(schema)
	_, err = schema_file.Write(serialized_schema)

	if err != nil {
		return err
	}

	return nil
}

func (tm *TableManager) GetTable(name string) (schema Schema, err error) {
	schema = Schema{}
	exist, err := tm.FileManager.FileExists(name)
	if err != nil {
		return schema, err
	}

	if !exist {
		return schema, errors.New("table does not exist")
	}

	data, err := tm.FileManager.ReadAll()
	if err != nil {
		return schema, err
	}
	schema = DeserializeSchema(data)

	return schema, nil
}
