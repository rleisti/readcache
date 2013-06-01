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
	return &lazycache{getter, make(map[string]*Cacheable), make(map[string]*sync.Once), new(sync.RWMutex), new(sync.RWMutex)}
}

// Type lazycache implements the Cache interface
type lazycache struct {
	Getter           func(string) Cacheable
	Cache            map[string]*Cacheable
	ReadControls     map[string]*sync.Once
	CacheLock        *sync.RWMutex
	ReadControlsLock *sync.RWMutex
}

// Get an item from the cache, retrieving the item from the getter if necessary.
// This implemention is meant to be goroutine safe.  It assumes that updating a
// map while concurrently reading from it is unsafe, so it uses a read/write mutex
// to synchronize access to its internal maps.
func (c *lazycache) Get(key string) interface{} {
	cachedValue, ok := getFromCache(c, key)
	if ok {
		return cachedValue.Value
	}

	readControl, cachedValue, ok := getReadControl(c, key)
	if ok {
		return cachedValue.Value
	}

	cachedValue = doFetch(c, key, readControl)
	return cachedValue.Value
}

// Attempt to retrieve an item from the cache, if it exists and hasn't expired.
// Returns somevalue, true if exists or nil, false if it does not.
func getFromCache(c *lazycache, key string) (*Cacheable, bool) {
	c.CacheLock.RLock()
	cachedValue, ok := c.Cache[key]
	c.CacheLock.RUnlock()
	if ok {
		now := time.Now()
		if cachedValue.ExpiresAt.After(now) {
			return cachedValue, true
		}
		c.CacheLock.Lock()
		// Determine if another goroutine has updated the cache before the lock
		cachedValue, ok = c.Cache[key]
		if ok && cachedValue.ExpiresAt.After(now) {
			return cachedValue, true
		}
		delete(c.Cache, key)
		c.CacheLock.Unlock()
	}
	return nil, false
}

// Get a Once for controlling the read-through on a particular cached item.
// Performs a last-minute check to determine if another goroutine has populated
// the cache before a lock is acquired, so this function may return a cached
// value instead.  If so, the third return value will be true.  Otherwise, a
// read control is returned and the third value is false.
func getReadControl(c *lazycache, key string) (readControl *sync.Once, cachedItem *Cacheable, gotCachedItem bool) {
	gotCachedItem = false

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
		cachedItem, ok = c.Cache[key]
		c.CacheLock.RUnlock()

		if ok {
			c.ReadControlsLock.Unlock()
			gotCachedItem = true
			return
		}

		readControl, ok = c.ReadControls[key]
		if !ok {
			readControl = new(sync.Once)
			c.ReadControls[key] = readControl
		}
		c.ReadControlsLock.Unlock()
	}

	return
}

// Use the given read control to fetch a value and store it in the cache.
// The read control may prevent this goroutine from fetching the value if
// some other routine gets to it first.  In either case, the resulting
// fetched value is returned.
func doFetch(c *lazycache, key string, readControl *sync.Once) (cachedValue *Cacheable) {
	fetchedValue := false
	readControl.Do(func() {
		defer func() {
			c.ReadControlsLock.Lock()
			delete(c.ReadControls, key)
			c.ReadControlsLock.Unlock()
		}()

		valueFromGetter := c.Getter(key)
		cachedValue = &valueFromGetter
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

	return
}
