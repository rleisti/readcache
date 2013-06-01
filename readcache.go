/*
Package readcache implements a read-through cache where the caller supplies
the means to read-through in the form of a function.

TODO: Add a maximum size, enforce with an LRU algorithm
*/
package readcache

import (
	"sync"
	"time"
)

// Type Cacheable is something that may be stored in a cache
type Cacheable struct {
	// The item in the cache
	Value interface{}

	// The time at which this item should expire from the cache.
	ExpiresAt time.Time
}

// Type Cache defines a read-through cache.
type Cache interface {
	// Retrieve an item from the cache if available, or from a
	// backing source if it is not.
	Get(key string) interface{}
}

// Constructs a new cache
func New(getter func(string) Cacheable) Cache {
	return &lazycache{getter, make(map[string]Cacheable), make(map[string]*sync.Once), new(sync.RWMutex), new(sync.RWMutex)}
}

// Type lazycache implements the Cache interface
type lazycache struct {
	Getter           func(string) Cacheable
	Cache            map[string]Cacheable
	ReadControls     map[string]*sync.Once
	CacheLock        *sync.RWMutex
	ReadControlsLock *sync.RWMutex
}

// Get an item from the cache, retrieving the item from the getter if necessary.
// This implemention is meant to be goroutine safe.  It assumes that updating a
// map while concurrently reading from it is unsafe, so it uses a read/write mutex
// to synchronize access to its internal maps.
func (c *lazycache) Get(key string) interface{} {
	c.CacheLock.RLock()
	cachedValue, ok := c.Cache[key]
	c.CacheLock.RUnlock()
	if ok {
		now := time.Now()
		if cachedValue.ExpiresAt.After(now) {
			return cachedValue.Value
		}
		c.CacheLock.Lock()
		// Determine if another goroutine has updated the cache before the lock
		cachedValue, ok = c.Cache[key]
		if ok && cachedValue.ExpiresAt.After(now) {
			return cachedValue
		}
		delete(c.Cache, key)
		c.CacheLock.Unlock()
	}

	c.ReadControlsLock.RLock()
	readControl, ok := c.ReadControls[key]
	c.ReadControlsLock.RUnlock()
	if !ok {
		c.ReadControlsLock.Lock()

		// Another goroutine may have created a read control, fetched an item, updated the
		// cache and cleaned up its read control by the time we reach this point.
		// Therefore, we verify that the cache still does not contain anything for the
		// given key.
		// Warning: possibility of deadlock when dealing with multiple locks.  Make sure
		//          they are always acquired in the same order.
		c.CacheLock.RLock()
		cachedValue, ok := c.Cache[key]
		c.CacheLock.RUnlock()

		if ok {
			c.ReadControlsLock.Unlock()
			return cachedValue.Value
		}

		readControl, ok = c.ReadControls[key]
		if !ok {
			readControl = new(sync.Once)
			c.ReadControls[key] = readControl
		}
		c.ReadControlsLock.Unlock()
	}

	fetchedValue := false
	readControl.Do(func() {
		defer func() {
			c.ReadControlsLock.Lock()
			delete(c.ReadControls, key)
			c.ReadControlsLock.Unlock()
		}()

		cachedValue = c.Getter(key)
		fetchedValue = true
		c.CacheLock.Lock()
		c.Cache[key] = cachedValue
		c.CacheLock.Unlock()
	})

	if !fetchedValue {
		c.CacheLock.RLock()
		cachedValue, _ = c.Cache[key]
		c.CacheLock.RUnlock()
	}

	return cachedValue.Value
}

