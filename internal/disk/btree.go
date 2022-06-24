package disk

import (
	"os"
	"time"
)

// Btree - Our in memory Btree struct
type Btree struct {
	root node
}

func (bt *Btree) Size() int {
	return bt.root.Size()
}

func (bt *Btree) IsRootNode(n node) bool {
	return bt.root == n
}

// NewBtree - Create a new btree
func InitializeBtree(path ...string) (*Btree, error) {
	if len(path) == 0 {
		path = make([]string, 1)
		path[0] = "./db/freedom.db"
	}

	file, err := os.OpenFile(path[0], os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dns := NewDiskNodeService(file)

	root, err := dns.getRootNodeFromDisk()
	if err != nil {
		panic(err)
	}
	return &Btree{root: root}, nil
}

// Insert - Insert element in tree
func (bt *Btree) Insert(value *Pairs) error {
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

func (bt *Btree) setRootNode(n node) {
	bt.root = n
}
