package crdt

import (
	"sync"
	"time"
)

type Element[T any] struct {
	Value     T
	Timestamp time.Time
}

// TimeMap is an implementation of a timeSet that uses a map data structure. We map items to timestamps.
type TimeMap[T any] struct {
	Storage map[string]Element[T] `json:"elements"`
	mutex   sync.RWMutex          // Maps in Go are not thread safe by default and that's why we use a mutex
}

// Add an element in the set if one of the following condition is met:
// - Given element does not exist yet
// - Given element already exists but with a lesser timestamp than the given one
func (s *TimeMap[T]) Add(key string, value T) error {
	t := time.Now()
	return s.AddWithTime(key, value, t)
}

// Add an element in the set if one of the following condition is met:
// - Given element does not exist yet
// - Given element already exists but with a lesser timestamp than the given one
func (s *TimeMap[T]) AddWithTime(key string, value T, t time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	elm, ok := s.Storage[key]
	addedAt := elm.Timestamp
	if !ok || (ok && t.After(addedAt)) {
		s.Storage[key] = Element[T]{Value: value, Timestamp: t}
	}
	return nil
}

// AddedAt returns the timestamp of a given element if it exists
//
// The second return value (bool) indicates whether the element exists or not
// If the given element does not exist, the second return (bool) is false
func (s *TimeMap[T]) AddedAt(key string) (time.Time, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	v, ok := s.Storage[key]
	return v.Timestamp, ok
}

// Each traverses the items in the Set, calling the provided function
// for each element key/value/timestamp association
func (s *TimeMap[T]) Each(f func(key string, val T, addedAt time.Time) error) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	for key, element := range s.Storage {
		err := f(key, element.Value, element.Timestamp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *TimeMap[T]) Size() int {
	size := 0
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	for range s.Storage {
		size++
	}
	return size
}

func (s *TimeMap[T]) Get(key string) (T, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	v, ok := s.Storage[key]
	return v.Value, ok
}

// newTimeSet returns an empty map-backed implementation of the CrdtEngine interface
func NewTimeSet[T comparable]() CrdtEngine[T] {
	return &TimeMap[T]{
		Storage: make(map[string]Element[T]),
	}
}
