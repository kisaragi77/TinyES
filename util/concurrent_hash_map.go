package util

import (
	"sync"

	farmhash "github.com/leemcloughlin/gofarmhash"
	"golang.org/x/exp/maps"
)

// Better Concurrent HashMap Than sync.Map
// Copyright (c) 2022-2024 @kisaragi77
type ConcurrentHashMap struct {
	mps   []map[string]any // Store map segments
	seg   int              // Number of map segments
	locks []sync.RWMutex   // Locks for each map
	seed  uint32           // Seed for farmhash initialization
}

// cap : the estimated capacity of the map
//
// seg : the number of segments
func NewConcurrentHashMap(seg, cap int) *ConcurrentHashMap {
	mps := make([]map[string]any, seg)
	locks := make([]sync.RWMutex, seg)
	for i := 0; i < seg; i++ {
		mps[i] = make(map[string]any, cap/seg)
	}
	return &ConcurrentHashMap{
		mps:   mps,
		seg:   seg,
		seed:  0,
		locks: locks,
	}
}

// Get hash index of the map by Key
func (m *ConcurrentHashMap) getSegIndex(key string) int {
	hash := int(farmhash.Hash32WithSeed([]byte(key), m.seed)) //FarmHash是google开源的Hash算法
	return hash % m.seg
}

// Set <key, value> to the map
func (m *ConcurrentHashMap) Set(key string, value any) {
	index := m.getSegIndex(key)
	m.locks[index].Lock()
	defer m.locks[index].Unlock()
	m.mps[index][key] = value
}

// Get value by key
func (m *ConcurrentHashMap) Get(key string) (any, bool) {
	index := m.getSegIndex(key)
	m.locks[index].RLock()
	defer m.locks[index].RUnlock()
	value, exists := m.mps[index][key]
	return value, exists
}

func (m *ConcurrentHashMap) CreateIterator() *ConcurrentHashMapIterator {
	keys := make([][]string, 0, len(m.mps))
	for _, mp := range m.mps {
		row := maps.Keys(mp)
		keys = append(keys, row)
	}
	return &ConcurrentHashMapIterator{
		cm:       m,
		keys:     keys,
		rowIndex: 0,
		colIndex: 0,
	}
}

type MapEntry struct {
	Key   string
	Value any
}

// MapIterator
type MapIterator interface {
	Next() *MapEntry
}

type ConcurrentHashMapIterator struct {
	cm       *ConcurrentHashMap
	keys     [][]string
	rowIndex int
	colIndex int
}

// Next returns the next element in the iteration.
func (iter *ConcurrentHashMapIterator) Next() *MapEntry {
	if iter.rowIndex >= len(iter.keys) {
		return nil
	}
	row := iter.keys[iter.rowIndex]
	if len(row) == 0 { //Null row
		iter.rowIndex += 1
		return iter.Next() // Find till the first non-null row
	}
	key := row[iter.colIndex]
	value, _ := iter.cm.Get(key)
	if iter.colIndex >= len(row)-1 {
		iter.rowIndex += 1
		iter.colIndex = 0
	} else {
		iter.colIndex += 1
	}
	return &MapEntry{key, value}
}
