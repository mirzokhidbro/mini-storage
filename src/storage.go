package src

import "rdbms/src/storage"

type StorageI interface {
	Table() storage.TableI
}

type Storage struct {
	table storage.TableI
}

func NewStorage(dataDir string) (StorageI, error) {
	tm, err := storage.NewTableManager(dataDir)
	if err != nil {
		return nil, err
	}
	return &Storage{table: tm}, nil
}

func (s *Storage) Table() storage.TableI {
	return s.table
}
