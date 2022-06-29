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
	GetChildOrSibling() (Node, error)
}

// Tree - Our in memory tree interface
type Tree interface {
	Size() int
	IsRootNode(n Node) bool
	SetRootNode(n Node)
	Insert(value *pair.Pairs) error
	Get(key string) (string, time.Time, bool, error)
	Next() bool
	Error() error
	IterGet() []*pair.Pairs
}
