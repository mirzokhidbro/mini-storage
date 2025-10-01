package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func SerializeSchema(schema *Schema) []byte {
	size := 2
	for _, col := range schema.Columns {
		size += 2 + len(col.Name)
		size += 2
		size += 2
	}

	buf := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(schema.Columns)))
	offset += 2

	for _, col := range schema.Columns {
		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(col.Name)))
		offset += 2

		copy(buf[offset:], []byte(col.Name))
		offset += len(col.Name)

		binary.LittleEndian.PutUint16(buf[offset:], uint16(col.Type))
		offset += 2

		binary.LittleEndian.PutUint16(buf[offset:], uint16(col.Length))
		offset += 2
	}

	return buf
}

func DeserializeSchema(schema []byte) Schema {
	columns := []Column{}
	offset := 2
	column_count := binary.LittleEndian.Uint16(schema[:offset])

	for column_count > 0 {
		name_length := binary.LittleEndian.Uint16(schema[offset : offset+2])
		offset += 2

		column_name := schema[offset : uint16(offset)+name_length]
		offset += int(name_length)

		column_type := binary.LittleEndian.Uint16(schema[offset : offset+2])
		offset += 2

		column_capacity := binary.LittleEndian.Uint16(schema[offset : offset+2])
		offset += 2

		columns = append(columns, Column{Name: string(column_name), Type: ColumnType(column_type), Length: int(column_capacity)})
		column_count--
	}

	return Schema{columns}
}

func SerializeRecord(schema Schema, record Record) []byte {
	column_count := len(schema.Columns)
	var buf bytes.Buffer

	for i := 0; i < column_count; i++ {
		switch schema.Columns[i].Type {
		case 0: // integer
			literal, ok := record.Items[i].Literal.(int)
			if ok {
				binary.Write(&buf, binary.LittleEndian, int64(literal))
			} else {
				fmt.Println("invalid data type for integer")
			}
		case 1: // varchar
			literal, ok := record.Items[i].Literal.(string)
			if ok {
				literal_length := len(literal)
				binary.Write(&buf, binary.LittleEndian, int16(literal_length))
				buf.Write([]byte(literal))
			} else {
				fmt.Println("invalid data type for string")
			}
		}
	}

	return buf.Bytes()
}

func DeserializeRecord(schema Schema, data []byte) Record {
	offset := 0
	items := make([]Item, 0, len(schema.Columns))

	for i := 0; i < len(schema.Columns); i++ {
		switch schema.Columns[i].Type {
		case 0: // int
			val := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
			items = append(items, Item{Literal: val})

		case 1: // varchar
			strlen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			str := string(data[offset : offset+strlen])
			offset += strlen
			items = append(items, Item{Literal: str})
		}
	}

	return Record{Items: items}
}
