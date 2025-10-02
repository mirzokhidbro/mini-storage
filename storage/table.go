package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
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

const PageSize = 8192

type PageHeader struct {
	RecordCount      uint16
	FreeSpacePointer uint16
}

type ItemPointer struct {
	Offset uint16
	Length uint16
}

type Page struct {
	Header PageHeader
	Data   [PageSize]byte
	Items  []ItemPointer
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

func (tm *TableManager) GetTableSchema(name string) (schema Schema, err error) {
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

func (tm *TableManager) Insert(schema Schema, record Record) error {
	serialized_record := SerializeRecord(schema, record)

	page, page_order, err := tm.FindOrCreatePage(serialized_record)

	fmt.Println("page order")
	fmt.Println(page_order)

	if err != nil {
		return err
	}

	err = tm.FileManager.Write("table", (int64(page_order)-1)*8192, page)

	if err != nil {
		return err
	}

	return nil
}

func (tm *TableManager) GetAllData(schema Schema) (records []Record, err error) {
	size, err := tm.FileManager.GetFileSize("table")
	if err != nil {
		return nil, err
	}

	pages_count := int(size / PageSize)
	binary_data, err := tm.FileManager.Read(0, size)

	if err != nil {
		return records, err
	}

	for i := 1; i <= pages_count; i++ {
		page := binary_data[(i-1)*PageSize : i*PageSize]
		record_count := uint16(binary.LittleEndian.Uint16(page[0:2]))

		offset := PageSize
		for record_count > 0 {
			record_offset := uint16(binary.LittleEndian.Uint16(page[offset-2 : offset]))

			offset -= 2
			record_length := uint16(binary.LittleEndian.Uint16(page[offset-2 : offset]))

			record := DeserializeRecord(schema, page[record_offset:record_offset+record_length])

			records = append(records, record)

			offset -= 2
			record_count--
		}
	}

	return records, nil
}

func (tm *TableManager) FindOrCreatePage(record []byte) (page []byte, page_order int, err error) {

	record_size := uint16(len(record))

	file_size, err := tm.FileManager.GetFileSize("table")

	if err != nil {
		return nil, 0, err
	}

	binary_data, err := tm.FileManager.Read(0, file_size)

	if err != nil {
		return nil, 0, nil
	}

	pages_count := int(len(binary_data) / PageSize)

	for i := 1; i <= pages_count; i++ {
		page_order = i
		page = binary_data[(i-1)*PageSize : i*PageSize]
		record_count := uint16(binary.LittleEndian.Uint16(page[:2]))
		free_space_pointer := uint16(binary.LittleEndian.Uint16(page[2:4]))

		free_space_length := PageSize - ((record_count+1)*4 + free_space_pointer)

		if free_space_length >= record_size {
			slot_beginning_address := (PageSize - record_count*4) - 4
			record_count++
			binary.LittleEndian.PutUint16(page[:2], uint16(record_count))
			copy(page[free_space_pointer:], record)

			fmt.Println(slot_beginning_address+2, slot_beginning_address+4)
			binary.LittleEndian.PutUint16(page[slot_beginning_address+2:slot_beginning_address+4], uint16(free_space_pointer))
			fmt.Println(slot_beginning_address, slot_beginning_address+2)
			binary.LittleEndian.PutUint16(page[slot_beginning_address:slot_beginning_address+2], record_size)

			free_space_pointer += record_size
			binary.LittleEndian.PutUint16(page[2:4], uint16(free_space_pointer))
			return page, i, nil
		}
	}

	page = make([]byte, PageSize)
	binary.LittleEndian.PutUint16(page[:2], uint16(1))
	binary.LittleEndian.PutUint16(page[2:4], uint16(4+len(record)))

	copy(page[4:], record)

	binary.LittleEndian.PutUint16(page[8190:8192], uint16(4))
	binary.LittleEndian.PutUint16(page[8188:8190], uint16(len(record)))

	return page, page_order + 1, nil
}
