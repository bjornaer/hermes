package types

import (
	"time"

	"github.com/bjornaer/hermes/internal/disk/pair"
)

// node - Interface for node
type Node interface {
	InsertPair(value *pair.Pairs, tree Tree) error
	GetValue(key string) (string, time.Time, error)
	PrintTree(level int)
	Size() int
	GetElements() []*pair.Pairs
}

// Tree - Our in memory tree interface
type Tree interface {
	Size() int
	IsRootNode(n Node) bool
	SetRootNode(n Node)
	Insert(value *pair.Pairs) error
	Get(key string) (string, time.Time, bool, error)
	Error() error
}

type DataPoint[T comparable] struct {
	ID        T
	Embedding []float64
}

type SearchResult[T comparable] struct {
	ID       string
	Distance float64
	Vector   []float64
}

func NewDataPoint[T comparable](id T, embedding []float64) *DataPoint[T] {
	return &DataPoint[T]{id, embedding}
}
