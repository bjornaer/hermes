package btree

import (
	"os"
	"time"

	"github.com/bjornaer/hermes/internal/disk/diskblock"
	"github.com/bjornaer/hermes/internal/disk/pair"
	"github.com/bjornaer/hermes/internal/disk/types"
)

type node = types.Node

func CreateOrOpenFile(path string) (*os.File, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.Create(path)
	}
	return os.Open(path)
}

// Btree - Our in memory Btree struct
type Btree[T any] struct {
	root node
	err  error
}

func (bt *Btree[T]) Size() int {
	return bt.root.Size()
}

func (bt *Btree[T]) IsRootNode(n node) bool {
	return bt.root == n
}

// NewBtree - Create a new btree
func InitializeBtree[T any](optionalPath ...string) (*Btree[T], error) {
	path := "./db/hermes/olympus.db"
	if len(optionalPath) != 0 {
		path = optionalPath[0]
	}

	file, err := CreateOrOpenFile(path)
	if err != nil {
		return nil, err
	}
	dns := diskblock.NewDiskNodeService(file)

	root, err := dns.GetRootNodeFromDisk()
	if root == nil || err != nil {
		panic(err)
	}
	return &Btree[T]{root: root, err: nil}, nil
}

// Insert - Insert element in tree
func (bt *Btree[T]) Insert(value *pair.Pairs) error {
	return bt.root.InsertPair(value, bt)
}

func (bt *Btree[T]) Get(key string) (string, time.Time, bool, error) {
	value, addedAt, err := bt.root.GetValue(key)
	if err != nil {
		return "", time.Time{}, false, err
	}
	if value == "" {
		return "", time.Time{}, false, nil
	}
	if addedAt.IsZero() {
		return "", time.Time{}, false, nil
	}
	return value, addedAt, true, nil
}

func (bt *Btree[T]) SetRootNode(n node) {
	bt.root = n
}

func (bt *Btree[T]) Iterate(f func(key string, val T, addedAt time.Time) error) error {
	return depthFirstPostOrder(bt.root, f)
}

func (bt *Btree[T]) Error() error {
	return bt.err
}

func depthFirstPostOrder[T any](node types.Node, f func(key string, val T, addedAt time.Time) error) error {
	diskNode := node.(*diskblock.DiskNode)
	children, err := diskNode.GetChildNodes()
	if err != nil {
		return err
	}
	for _, child := range children {
		depthFirstPostOrder(child, f)
	}
	for _, elm := range diskNode.GetElements() {
		err := f(elm.Key, any(elm.Value).(T), elm.Timestamp)
		if err != nil {
			return err
		}
	}
	return nil
}
