package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dataDir string // Ma'lumotlar saqlanadigan papka
	lock    sync.Mutex
}

func NewFileManager(dataDir string) (*FileManager, error) {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}
	return &FileManager{dataDir: dataDir}, nil
}

func (fm *FileManager) Write(fileName string, offset int64, data []byte) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, fileName)

	flags := os.O_RDWR | os.O_CREATE

	if offset == 0 {
		flags |= os.O_TRUNC
	}

	file, err := os.OpenFile(filePath, flags, 0666)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	defer file.Close()

	if offset == 0 {
		_, err = file.Write(data)
	} else {
		_, err = file.WriteAt(data, offset)
	}

	if err != nil {
		return fmt.Errorf("failed to write to file %s: %v", fileName, err)
	}

	return nil
}

func (fm *FileManager) Read(fileName string, offset int64, size int) ([]byte, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, fileName)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	defer file.Close()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read from file %s: %v", fileName, err)
	}

	return data[:n], nil
}

func (fm *FileManager) Close() error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	return nil
}

func (fm *FileManager) FileExists(name string) (bool, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, name)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (fm *FileManager) CreateFile(name string) (*os.File, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, name)
	return os.Create(filePath)
}

func (fm *FileManager) DeleteFile(name string) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, name)
	return os.Remove(filePath)
}

func (fm *FileManager) ListFiles(extension string) ([]string, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	files, err := os.ReadDir(fm.dataDir)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), extension) {
			result = append(result, file.Name())
		}
	}

	return result, nil
}

func (fm *FileManager) OpenFile(name string) (*os.File, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, name)
	return os.OpenFile(filePath, os.O_RDWR, 0666)
}

func (fm *FileManager) GetFileInfo(name string) (os.FileInfo, error) {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	filePath := filepath.Join(fm.dataDir, name)
	return os.Stat(filePath)
}
