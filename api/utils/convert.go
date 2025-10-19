package utils

import (
	"errors"

	"rdbms/api/models"
	"rdbms/src/storage"
)

func ToStorageSchema(req models.CreateTableRequest) (storage.Schema, error) {
	if req.Name == "" {
		return storage.Schema{}, errors.New("table name is required")
	}
	if len(req.Columns) == 0 {
		return storage.Schema{}, errors.New("at least one column is required")
	}

	columns := make([]storage.Column, 0, len(req.Columns))

	for _, c := range req.Columns {
		colName := c.Name
		if colName == "" {
			return storage.Schema{}, errors.New("column name is required")
		}

		var column_type storage.ColumnType
		var length int
		switch c.Type {
		case 0:
			column_type = storage.TypeInt
			length = 0
		case 1:
			column_type = storage.TypeVarchar
			if c.Length == nil {
				length = 255
			} else {
				length = *c.Length
				if length < 0 {
					return storage.Schema{}, errors.New("invalid length for varchar column")
				}
			}
		case 2:
			column_type = storage.TypeDate
			length = 0
		case 3:
			column_type = storage.TypeTimestamp
			length = 0
		case 4:
			column_type = storage.TypeFloat
			length = 0
		case 5:
			column_type = storage.TypeJSON
			length = 0
		default:
			return storage.Schema{}, errors.New("unsupported type")
		}

		columns = append(columns, storage.Column{
			Name:   colName,
			Type:   column_type,
			Length: length,
		})
	}

	return storage.Schema{Columns: columns}, nil
}
