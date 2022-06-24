package server

import (
	"github.com/bjornaer/hermes/internal/crdt"
	"github.com/bjornaer/hermes/internal/disk"
)

// addition := NewTimeSet[T]()
// removal := NewTimeSet[T]()

//DB - Handle exported by the package
type DB[T comparable] struct {
	Crdt *crdt.LastWriterWinsSet[T]
}

//Open - Opens a new db connection at the file path
func Open[T comparable](filePath string) (*DB, error) {
	addition, err := disk.InitializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	removal, err := disk.InitializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	storage := crdt.NewLWWSet[T](addition, removal) // disk.InitializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	return &DB[T]{&storage}, nil
}

//Put - Insert a key value pair in the database
func (db *DB) Put(key string, value string) error {
	pair := NewPair(key, value)
	if err := pair.Validate(); err != nil {
		return err
	}
	db.storage.Add()
	return db.storage.Insert(pair)
}

//Put - Insert a key value pair in the database
func (db *DB) Delete(key string, value string) error {
	pair := NewPair(key, value)
	if err := pair.Validate(); err != nil {
		return err
	}
	return db.storage.Insert(pair)
}

//Get - Get the stored value from the database for the respective key
func (db *DB) Get(key string) (string, bool, error) {
	return db.storage.Get(key)
}
