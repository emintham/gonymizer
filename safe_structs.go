package gonymizer

import (
	"github.com/google/uuid"
	"sync"
)

// safeAlphaNumericMap is a concurrency-safe map[string]map[string][string]
type safeAlphaNumericMap struct {
	v   map[string]map[string]string
	mux sync.Mutex
}

// Get returns a string that an input is mapped to under a parentKey.
func (c *safeAlphaNumericMap) Get(parentKey, input string) (string, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	anMap, ok := c.v[parentKey]
	if !ok {
		anMap = map[string]string{}
		c.v[parentKey] = anMap
	}

	result, ok := anMap[input]
	if !ok {
		result = scrambleString(input)
		anMap[input] = result
	}

	return result, nil
}

// safeUUIDMap is a concurrency-safe map[uuid.UUID]uuid.UUID
type safeUUIDMap struct {
	v   map[uuid.UUID]uuid.UUID
	mux sync.Mutex
}

// Get returns a mapped uuid for a given UUID if it has already previously been anonymized and a new UUID otherwise.
func (c *safeUUIDMap) Get(key uuid.UUID) (string, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	result, ok := c.v[key]
	if !ok {
		result, err := uuid.NewRandom()
		if err != nil {
			return "", err
		}

		c.v[key] = result
	}

	return result.String(), nil
}

// safeCounter is a concurrency-safe map[string]int
type safeCounter struct {
	v   map[string]int
	mux sync.Mutex
}

// Inc increments the counter for the given key and returns its previous value
func (c *safeCounter) Inc(key string) int {
	c.mux.Lock()
	defer c.mux.Unlock()

	counter, _ := c.v[key]
	c.v[key]++
	return counter
}
