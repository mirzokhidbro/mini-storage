package storage

type Page struct {
	ID       int64
	Data     []byte
	Metadata map[string]interface{}
}
