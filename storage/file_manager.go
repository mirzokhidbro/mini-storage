package storage

import (
	"os"
	"sync"
)

type FileManager struct {
	file *os.File
	lock sync.Mutex
}

func NewFileManager(filePath string) (*FileManager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &FileManager{file: file}, nil
}

func (fm *FileManager) Write(offset int64, data []byte) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	_, err := fm.file.WriteAt(data, offset)
	return err
}

func (fm *FileManager) Read(offset int64, size int) ([]byte, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	data := make([]byte, size)
	_, err := fm.file.ReadAt(data, offset)
	return data, err
}
