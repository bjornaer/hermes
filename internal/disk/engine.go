package disk

import (
	"time"

	"github.com/bjornaer/hermes/internal/crdt"
)

//DB - Handle exported by the package
type DiskStorage struct {
	storage *Btree
}

//Open - Opens a new db connection at the file path
func Open(filePath string) (*DiskStorage, error) {
	storage, err := InitializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	return &DiskStorage{storage}, nil
}

//Get - Get the stored value from the database for the respective key
func (ds *DiskStorage) Get(key string) (string, bool) {
	v, _, found, err := ds.storage.Get(key)
	if err != nil {
		return "", false
	}
	return v, found
}

func (ds *DiskStorage) Add(key string, value string) error {
	pair := NewPair(key, value)
	if err := pair.Validate(); err != nil {
		return err
	}
	return ds.storage.Insert(pair)
}

// AddedAt returns the timestamp of a given element if it exists
//
// The second return value (bool) indicates whether the element exists or not
// If the given element does not exist, the second return (bool) is false
func (ds *DiskStorage) AddedAt(key string) (time.Time, bool) {
	_, t, found, err := ds.storage.Get(key)
	if err != nil {
		return time.Time{}, false
	}
	return t, found
}

// Each traverses the items in the Set, calling the provided function
// for each element key/value/timestamp association
func (ds *DiskStorage) Each(f func(key string, val string, addedAt time.Time) error) error {
	for key, element := range ds.storage { // this gonna be a fuckaroo to make happen
		err := f(key, element.Value, element.Timestamp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ds *DiskStorage) Size() int {
	return ds.storage.Size()
}

// NewDiskStorage returns an empty memory DB storage implementation of the CrdtEngine interface
func NewDiskStorage[T any](filePath string) (crdt.CrdtEngine[T], error) {
	storage, err := InitializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	return &DiskStorage{storage}, nil
}
