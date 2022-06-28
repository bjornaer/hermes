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
type Btree struct {
	root node
	node node
	err  error
}

func (bt *Btree) Size() int {
	return bt.root.Size()
}

func (bt *Btree) IsRootNode(n node) bool {
	return bt.root == n
}

// NewBtree - Create a new btree
func InitializeBtree(optionalPath ...string) (*Btree, error) {
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
	return &Btree{root: root, node: root, err: nil}, nil
}

// Insert - Insert element in tree
func (bt *Btree) Insert(value *pair.Pairs) error {
	return bt.root.InsertPair(value, bt)
}

func (bt *Btree) Get(key string) (string, time.Time, bool, error) {
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

func (bt *Btree) SetRootNode(n node) {
	bt.root = n
}

func (bt *Btree) Next() bool {
	next, err := bt.node.GetChildOrSibling()
	if next == nil {
		bt.node = bt.root
		return false
	}
	if err != nil {
		bt.err = err
		bt.node = bt.root
		return false
	}
	bt.node = next
	return true
}

func (bt *Btree) Error() error {
	return bt.err
}

func (bt *Btree) IterGet() []*pair.Pairs {
	return bt.node.GetElements()
}
