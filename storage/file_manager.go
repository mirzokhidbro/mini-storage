package storage

import (
	"errors"
	"io"
	"os"
	"sync"
)

type FileManager struct {
	file *os.File
	lock sync.Mutex
}

func NewFileManager(filePath string) (*FileManager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return &FileManager{file: nil}, nil
		}
		return nil, err
	}
	return &FileManager{file: file}, nil
}

func (fm *FileManager) Write(offset int64, data []byte) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	_, err := fm.file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	return fm.file.Sync()
}

func (fm *FileManager) Read(offset int64, size int) ([]byte, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	data := make([]byte, size)
	n, err := fm.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (fm *FileManager) ReadAll() ([]byte, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	if _, err := fm.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return io.ReadAll(fm.file)
}

func (fm *FileManager) Close() error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	return fm.file.Close()
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

func (fm *FileManager) GetFileSize(name string) (int64, error) {
	exist, err := fm.FileExists(name)
	if err != nil {
		return 0, err
	}

	if !exist {
		return 0, errors.New("file does not exist")
	}

	info, err := fm.file.Stat()

	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
