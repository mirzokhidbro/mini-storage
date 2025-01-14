package storage

type Block struct {
	ID    int64
	Data  []byte
	Dirty bool
}
