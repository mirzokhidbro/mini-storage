package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type FileManager struct {
	recordFile *os.File
	schemaFile *os.File
	lock       sync.Mutex
}

func NewFileManager(fileName string) (*FileManager, error) {
	var recordFile, schemaFile *os.File
	var err error

	recordFile, err = os.OpenFile(fileName+".table", os.O_RDWR, 0666)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		recordFile = nil
	}

	schemaFile, err = os.OpenFile(fileName+".schema", os.O_RDWR, 0666)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		schemaFile = nil
	}

	return &FileManager{recordFile: recordFile, schemaFile: schemaFile}, nil
}

func (fm *FileManager) Write(fileType string, offset int64, data []byte) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	if fileType == "schema" {
		_, err := fm.schemaFile.WriteAt(data, offset)
		if err != nil {
			return err
		}
		return fm.schemaFile.Sync()
	} else {
		_, err := fm.recordFile.WriteAt(data, offset)
		if err != nil {
			return err
		}
		return fm.recordFile.Sync()
	}
}

func (fm *FileManager) Read(offset int64, size int64) ([]byte, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	data := make([]byte, size)
	n, err := fm.recordFile.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (fm *FileManager) ReadAll() ([]byte, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	if _, err := fm.schemaFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return io.ReadAll(fm.schemaFile)
}

func (fm *FileManager) Close(fileType string) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	return fm.schemaFile.Close()
}

func (fm *FileManager) FileExists(name string) (bool, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (fm *FileManager) CreateFile(name string) (*os.File, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	return os.Create(name)
}

func (fm *FileManager) DeleteFile(name string) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	return os.Remove(name)
}

func (fm *FileManager) OpenFile(name string) (*os.File, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	return os.OpenFile(name, os.O_RDWR, 0666)
}

func (fm *FileManager) GetFileSize(fileType string) (int64, error) {
	if fileType == "schema" {
		exist, err := fm.FileExists(fm.schemaFile.Name())
		if err != nil {
			return 0, err
		}

		if !exist {
			return 0, errors.New("file does not exist")
		}

		info, err := fm.schemaFile.Stat()

		if err != nil {
			return 0, err
		}

		return info.Size(), nil
	}

	fmt.Println("record file name")
	fmt.Println(fm.recordFile.Name())

	exist, err := fm.FileExists(fm.recordFile.Name())
	if err != nil {
		return 0, err
	}

	if !exist {
		return 0, errors.New("file does not exist")
	}

	info, err := fm.recordFile.Stat()

	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
