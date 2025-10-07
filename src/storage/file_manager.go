package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

type FileManager struct {
	root  string
	files map[string]*os.File
}

func NewFileManager(root string) (*FileManager, error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(root, 0o755); mkErr != nil {
			return nil, mkErr
		}
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}

	files := make(map[string]*os.File)

	for _, e := range entries {
		if !e.IsDir() {
			fullPath := filepath.Join(root, e.Name())
			f, err := os.OpenFile(fullPath, os.O_RDWR, 0666)
			if err != nil {
				return nil, err
			}

			base := filepath.Base(f.Name())

			files[base] = f
		}
	}

	return &FileManager{root: root, files: files}, nil
}

func (fm *FileManager) Write(fileName string, offset int64, data []byte) error {

	file := fm.files[fileName]

	_, err := file.WriteAt(data, offset)

	if err != nil {
		return err
	}

	return nil
}

func (fm *FileManager) Read(fileName string, offset int64, size int64) ([]byte, error) {
	data := make([]byte, size)
	file := fm.files[fileName]
	n, err := file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (fm *FileManager) ReadAll(fileName string) ([]byte, error) {

	file := fm.files[fileName]

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return io.ReadAll(file)
}

func (fm *FileManager) FileExists(name string) bool {

	_, ok := fm.files[name]
	return ok
}

func (fm *FileManager) CreateFile(name string) (*os.File, error) {
	full := filepath.Join(fm.root, name)
	file, err := os.Create(full)
	if err != nil {
		return nil, err
	}
	fm.files[name] = file
	return file, nil
}

func (fm *FileManager) DeleteFile(name string) error {
	return os.Remove(filepath.Join(fm.root, name))
}

func (fm *FileManager) OpenFile(name string) (*os.File, error) {
	return os.OpenFile(filepath.Join(fm.root, name), os.O_RDWR, 0666)
}

func (fm *FileManager) GetFileSize(fileName string) (int64, error) {
	file := fm.files[fileName]

	if !fm.FileExists(fileName) {
		return 0, errors.New("file does not exist")
	}

	info, err := file.Stat()

	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
