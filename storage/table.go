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
	fileManager *FileManager
}

func NewTableManager(filePath string) (*TableManager, error) {
	file_manager, err := NewFileManager(filePath)
	if err != nil {
		panic(err.Error())
	}
	return &TableManager{fileManager: file_manager}, nil
}

func (tm *TableManager) CreateTable(name string, schema *Schema) error {
	exist, err := tm.fileManager.FileExists(name + ".schema")

	if err != nil {
		return err
	}

	if exist {
		return errors.New("file already exist")
	}

	file, err := tm.fileManager.CreateFile(name + ".schema")

	if err != nil {
		return err
	}

	serialized_schema := SerializeSchema(schema)
	_, err = file.Write(serialized_schema)

	if err != nil {
		return err
	}

	return nil
}

func (tm *TableManager) GetTable(name string) (schema Schema, err error) {
	schema = Schema{}
	exist, err := tm.fileManager.FileExists(name)
	if err != nil {
		return schema, err
	}

	if !exist {
		return schema, errors.New("table does not exist")
	}

	data, err := tm.fileManager.ReadAll()
	if err != nil {
		return schema, err
	}
	schema = DeserializeSchema(data)

	return schema, nil
}
