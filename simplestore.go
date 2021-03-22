package chunkreaderat

import (
	"errors"
	"time"

	"github.com/eko/gocache/store"
)

type SimpleStore struct {
	key   interface{}
	value interface{}
}

func NewSimpleStore() *SimpleStore {
	return &SimpleStore{}
}

func (s *SimpleStore) GetWithTTL(key interface{}) (interface{}, time.Duration, error) {
	i, err := s.Get(key)
	return i, 10 * time.Minute, err
}

func (s *SimpleStore) Get(key interface{}) (interface{}, error) {
	if s.key == key {
		return s.value, nil
	}
	return nil, errors.New("Unable to retrieve data")
}

func (s *SimpleStore) Set(key interface{}, value interface{}, options *store.Options) error {
	s.key = key
	s.value = value
	return nil
}

func (s *SimpleStore) Delete(key interface{}) error {
	if s.key == key {
		s.key = nil
		s.value = nil
	}
	return errors.New("Unable to find key")
}

func (s *SimpleStore) Invalidate(options store.InvalidateOptions) error {
	return nil
}

func (s *SimpleStore) Clear() error {
	s.key = nil
	s.value = nil
	return nil
}

func (s *SimpleStore) GetType() string {
	return "SimpleCache"
}
