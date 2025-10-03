package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

type FileManager struct {
	files map[string]*os.File
}

func NewFileManager(fileName string) (*FileManager, error) {
	path := "data"
	entries, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	files := make(map[string]*os.File)

	for _, e := range entries {
		if !e.IsDir() {
			fullPath := filepath.Join(path, e.Name())
			f, err := os.OpenFile(fullPath, os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}

			fileName := filepath.Base(f.Name())

			// fmt.Println("files")
			// fmt.Println(fileName)

			files[fileName] = f
		}
	}

	return &FileManager{files: files}, nil
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

	// for i, f_name := range fm.files {
	// 	fmt.Println("exist files")
	// 	fmt.Println(i)
	// 	fmt.Println("file name")
	// 	fmt.Println(f_name)
	// 	// fmt.Println(fm.files[name])
	// }

	_, ok := fm.files[name]
	return ok
}

func (fm *FileManager) CreateFile(name string) (*os.File, error) {
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	fm.files[name] = file
	return file, nil
}

func (fm *FileManager) DeleteFile(name string) error {
	return os.Remove(name)
}

func (fm *FileManager) OpenFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDWR, 0666)
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
