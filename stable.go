package studying_raft

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type StableStore interface {
	Set(key []byte, val []byte) error

	// Get returns the value for key, or an empty byte slice if key was not found.
	Get(key []byte) ([]byte, error)

	SetUint64(key []byte, val uint64) error

	// GetUint64 returns the uint64 value for key, or 0 if key was not found.
	GetUint64(key []byte) (uint64, error)
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu      sync.Mutex
	mByte   map[string][]byte
	mUint64 map[string]uint64
}

func NewMapStorage() *MapStorage {
	mByte := make(map[string][]byte)
	mUint64 := make(map[string]uint64)

	return &MapStorage{
		mByte:   mByte,
		mUint64: mUint64,
	}
}

func (ms *MapStorage) Get(key []byte) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.mByte[string(key)]
	if !found {
		return v, ErrKeyNotFound
	}

	return v, nil
}

func (ms *MapStorage) Set(key []byte, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.mByte[string(key)] = value

	return nil
}

func (ms *MapStorage) SetUint64(key []byte, val uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.mUint64[string(key)] = val

	return nil
}

func (ms *MapStorage) GetUint64(key []byte) (uint64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.mUint64[string(key)]
	if !found {
		return v, ErrKeyNotFound
	}

	return v, nil
}

func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.mByte) > 0
}
