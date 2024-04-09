package disk

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/bjornaer/hermes/internal/disk/btree"
	"github.com/bjornaer/hermes/internal/disk/pair"
	"github.com/bjornaer/hermes/internal/disk/types"
	"github.com/bjornaer/hermes/internal/disk/vector"
)

// DiskStorage is the representation of our storage logical unit, built on top of our VectorIndex and B Tree
type DiskStorage[T comparable] struct {
	storage         *btree.Btree[T]
	distanceMeasure vector.DistanceMeasure
	// vectorIndex *vector.VectorIndex[T]
}

// Get - Get the stored value from the database for the respective key // FIXME this any casting bs is to avoid handling generics inside BTREE code
func (ds *DiskStorage[T]) Get(id string) ([]float64, bool) {
	v, _, found, err := ds.storage.Get(id)

	if err != nil {
		return []float64{}, false
	}

	str := any(v).(string)
	emb, err := vector.ConvertStrToEmbedding(str)
	if err != nil {
		return []float64{}, false
	}
	return emb, found
}

func (ds *DiskStorage[T]) Add(dp types.DataPoint[T]) error {
	key := any(dp.ID).(string)
	v := vector.ConvertFloat64ArrToStr(dp.Embedding)

	pair := pair.NewPair(key, v)
	if err := pair.Validate(); err != nil {
		return err
	}
	return ds.storage.Insert(pair)
}

func (ds *DiskStorage[T]) AddWithTime(dp types.DataPoint[T], t time.Time) error {
	key := any(dp.ID).(string)
	v := vector.ConvertFloat64ArrToStr(dp.Embedding)
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
func (ds *DiskStorage[T]) AddedAt(id string) (time.Time, bool) {
	_, t, found, err := ds.storage.Get(id)
	if err != nil {
		return time.Time{}, false
	}
	return t, found
}

// Each traverses the items in the Tree, calling the provided function
// for each element key/value/timestamp association
func (ds *DiskStorage[T]) Each(f func(key, val string, addedAt time.Time) error) error {
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

func (ds *DiskStorage[T]) SearchByVector(input []float64, limit int) (*[]types.SearchResult[T], error) {
	count, err := ds.storage.Count()
	if err != nil {
		return nil, err
	}
	// calculate distances
	idToDist := make(map[string]float64, count)
	ann := make([]string, 0, count)
	ds.Each(
		func(key, val string, addedAt time.Time) error {
			ann = append(ann, key)
			emb, err := vector.ConvertStrToEmbedding(val)
			if err != nil {
				return err
			}
			idToDist[key] = ds.distanceMeasure.CalcDistance(emb, input)
			return nil
		},
	)

	// sort the found items by their actual distance
	sort.Slice(ann, func(i, j int) bool {
		return idToDist[ann[i]] < idToDist[ann[j]]
	})

	// return the top n items
	if len(ann) > limit {
		ann = ann[:limit]
	}

	searchResults := make([]types.SearchResult[T], len(ann))
	for i, id := range ann {
		emb, found := ds.Get(id)
		if !found {
			return nil, fmt.Errorf("embedding not found for id %s", id)
		}
		searchResults[i] = types.SearchResult[T]{ID: id, Distance: math.Abs(idToDist[id]), Vector: emb}
	}

	return &searchResults, nil
}

// NewDiskStorage returns an empty memory DB storage implementation of the CrdtEngine interface
// TODO: ensure dimension size is respected
func NewDiskStorage[T comparable](filePath ...string) (*DiskStorage[T], error) {
	var storage *btree.Btree[T]
	var err error
	if len(filePath) != 0 {
		storage, err = btree.InitializeBtree[T](filePath[0])
	} else {
		storage, err = btree.InitializeBtree[T]()
	}
	if err != nil {
		return nil, err
	}

	// vi, err := vector.NewVectorIndex[T](storage, 1, 3, vector.NewCosineDistanceMeasure())
	// if err != nil {
	// 	return nil, err
	// }

	return &DiskStorage[T]{storage: storage}, nil
}
