package crdt

import "time"

type CrdtEngine[T any] interface {
	Add(string, T) error
	AddWithTime(string, T, time.Time) error
	AddedAt(string) (time.Time, bool)
	Each(func(string, T, time.Time) error) error
	Size() int
	Get(string) (T, bool)
}
