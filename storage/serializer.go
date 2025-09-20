package storage

import "encoding/binary"

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
