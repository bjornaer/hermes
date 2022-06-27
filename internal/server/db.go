package server

import (
	"github.com/bjornaer/hermes/internal/crdt"
)

//Server - Handle exported by the package
type Server struct {
	Crdt crdt.LastWriterWinsSet[string]
}

//Put - Insert a key value pair in the database
func (s *Server) Put(key string, value string) error {
	return s.Crdt.Add(key, value)
}

//Delete - Remove a key value pair from the database
func (s *Server) Delete(key string, value string) error {
	return s.Crdt.Remove(key, value)
}

//Get - Get the stored value from the database for the respective key
func (s *Server) Get(key string) (string, bool) {
	return s.Crdt.Get(key)
}

func NewServer(c crdt.LastWriterWinsSet[string]) *Server {
	return &Server{Crdt: c}
}
