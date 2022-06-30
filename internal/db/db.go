package server

import (
	"github.com/bjornaer/hermes/internal/crdt"
)

//DB - Handle exported by the package
type DB struct {
	Crdt crdt.LastWriterWinsSet[string]
}

//Put - Insert a key value pair in the database
func (s *DB) Put(key string, value string) error {
	return s.Crdt.Add(key, value)
}

//Delete - Remove a key value pair from the database
func (s *DB) Delete(key string, value string) error {
	return s.Crdt.Remove(key, value)
}

//Get - Get the stored value from the database for the respective key
func (s *DB) Get(key string) (string, bool) {
	return s.Crdt.Get(key)
}

func NewDB(c crdt.LastWriterWinsSet[string]) *DB {
	return &DB{Crdt: c}
}
