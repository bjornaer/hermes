package disk

import (
	"fmt"
	"time"

	"github.com/bjornaer/hermes/internal/disk/btree"
	"github.com/bjornaer/hermes/internal/disk/pair"
)

//DB - Handle exported by the package
type DiskStorage[T any] struct {
	storage *btree.Btree[T]
}

//Get - Get the stored value from the database for the respective key // FIXME this any casting bs is to avoid handling generics inside BTREE code
func (ds *DiskStorage[T]) Get(key string) (T, bool) {
	v, _, found, err := ds.storage.Get(key)
	if err != nil {
		return any(v).(T), false
	}
	return any(v).(T), found
}

func (ds *DiskStorage[T]) Add(key string, value T) error {
	v, ok := any(value).(string)
	if !ok {
		return fmt.Errorf("We just store strings for now, sorry.")
	}
	pair := pair.NewPair(key, v)
	if err := pair.Validate(); err != nil {
		return err
	}
	return ds.storage.Insert(pair)
}

func (ds *DiskStorage[T]) AddWithTime(key string, value T, t time.Time) error {
	v, ok := any(value).(string)
	if !ok {
		return fmt.Errorf("We just store strings for now, sorry.")
	}
	pair := pair.NewPairWithTime(key, v, t)
	if err := pair.Validate(); err != nil {
		return err
	}
	return ds.storage.Insert(pair)
}

// AddedAt returns the timestamp of a given element if it exists
//
// The second return value (bool) indicates whether the element exists or not
// If the given element does not exist, the second return (bool) is false
func (ds *DiskStorage[T]) AddedAt(key string) (time.Time, bool) {
	_, t, found, err := ds.storage.Get(key)
	if err != nil {
		return time.Time{}, false
	}
	return t, found
}

// Each traverses the items in the Tree, calling the provided function
// for each element key/value/timestamp association
func (ds *DiskStorage[T]) Each(f func(key string, val T, addedAt time.Time) error) error {
	s := ds.storage
	err := s.Iterate(f)
	if err != nil {
		return err
	}
	if err := s.Error(); err != nil {
		return err
	}
	return nil
}

func (ds *DiskStorage[T]) Size() int {
	return ds.storage.Size()
}

// NewDiskStorage returns an empty memory DB storage implementation of the CrdtEngine interface
func NewDiskStorage[T any](filePath string) (*DiskStorage[T], error) {
	storage, err := btree.InitializeBtree[T](filePath)
	if err != nil {
		return nil, err
	}
	return &DiskStorage[T]{storage}, nil
}
