package crdt

import (
	"time"
)

type LastWriterWinsSet[T any] interface {
	Add(string, T) error
	Remove(string, T) error
	Exists(string) bool
	Get(string) (T, bool)
	GetAll() (map[string]T, error)
	Merge(LastWriterWinsSet[T]) error
	GetAdditions() CrdtEngine[T]
	GetRemovals() CrdtEngine[T]
}

// LWWSet is a Last-Writer-Wins Set implementation
type LWWSet[T any] struct {
	Additions CrdtEngine[T] `json:"additions"`
	Removals  CrdtEngine[T] `json:"removals"`
}

// Add marks an element to be added at a given timestamp
func (s *LWWSet[T]) Add(key string, value T) error {
	return s.Additions.Add(key, value)
}

// Add marks an element to be added at a given timestamp
func (s *LWWSet[T]) addWithTime(key string, value T, t time.Time) error {
	return s.Additions.AddWithTime(key, value, t)
}

func (s *LWWSet[T]) GetAdditions() CrdtEngine[T] {
	return s.Additions
}

// Remove marks an element to be removed at a given timestamp
func (s *LWWSet[T]) Remove(key string, value T) error {
	return s.Removals.Add(key, value)
}

// Remove marks an element to be removed at a given timestamp
func (s *LWWSet[T]) removeWithTime(key string, value T, t time.Time) error {
	return s.Removals.AddWithTime(key, value, t)
}

func (s *LWWSet[T]) GetRemovals() CrdtEngine[T] {
	return s.Removals
}

// Exists checks if an element is marked as present in the set
func (s *LWWSet[T]) Exists(key string) bool {
	addedAt, added := s.Additions.AddedAt(key)

	removed := s.isRemoved(key, addedAt)

	return added && !removed
}

// isRemoved checks if an element is marked for removal
func (s *LWWSet[T]) isRemoved(key string, since time.Time) bool {
	removedAt, removed := s.Removals.AddedAt(key)

	if !removed {
		return false
	}
	if since.Before(removedAt) {
		return true
	}
	return false
}

func (s *LWWSet[T]) Get(key string) (T, bool) {
	var empty T
	if s.Exists(key) {
		return s.Additions.Get(key)
	}
	return empty, false
}

// Get returns set content
func (s *LWWSet[T]) GetAll() (map[string]T, error) {
	var result map[string]T

	err := s.Additions.Each(func(key string, value T, addedAt time.Time) error {
		removed := s.isRemoved(key, addedAt)

		if !removed {
			result[key] = value
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// Merge additions and removals from other LWWSet into current set
func (s *LWWSet[T]) Merge(other LastWriterWinsSet[T]) error {
	err := other.GetAdditions().Each(func(key string, value T, addedAt time.Time) error {
		err := s.addWithTime(key, value, addedAt)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	err = other.GetRemovals().Each(func(key string, value T, addedAt time.Time) error {
		err := s.removeWithTime(key, value, addedAt)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// NewLWWSet returns an implementation of a LastWriterWinsSet
func NewLWWSet[T comparable](addition, removal CrdtEngine[T]) LastWriterWinsSet[T] {
	return &LWWSet[T]{
		Additions: addition,
		Removals:  removal,
	}
}
