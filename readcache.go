/*
Package readcache implements a read-through cache where the caller supplies
the means to read-through in the form of a function.

TODO: Add a mechanism for allowing the getter to return an error instead of an item

TODO: Add a maximum size; enforce with an LRU algorithm

TODO: Add a test for concurrent performance
*/
package readcache

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Type cacheable is something that may be stored in a cache
type cacheable struct {
	// The item in the cache
	Value interface{}

	// The time at which this item should expire from the cache.
	ExpiresAt time.Time
}

// Type Cache defines a read-through cache.
type Cache interface {
	// Retrieve an item from the cache if available, or from a
	// backing source if it is not.
	// May return an error instead, if the item cannot be fetched.
	Get(key string) (interface{}, error)
}

// Constructs a new cache
func New(getter func(string) (interface{}, time.Time, error)) Cache {
	return &readcache{getter, make(map[string]*cacheable), make(map[string]*sync.Once), new(sync.RWMutex), new(sync.RWMutex)}
}

// Type readcache implements the Cache interface
type readcache struct {
	// The fetcher of items
	Getter func(string) (interface{}, time.Time, error)

	// The cache of items
	Cache map[string]*cacheable

	// Controls read of items from the getter; prevents multiple concurrent reads of the same item.
	ReadControls map[string]*sync.Once

	// Locks the entire cache for reads or writes
	CacheLock *sync.RWMutex

	// Locks the read control manifest for reads or writes
	ReadControlsLock *sync.RWMutex
}

// Get an item from the cache, retrieving the item from the getter if necessary.
// This implemention is meant to be goroutine safe.  It assumes that updating a
// map while concurrently reading from it is unsafe, so it uses a read/write mutex
// to synchronize access to its internal maps.
func (c *readcache) Get(key string) (interface{}, error) {
	cachedValue, ok := getFromCache(c, key)
	if ok {
		return cachedValue.Value, nil
	}

	readControl, cachedValue, ok := getReadControl(c, key)
	if ok {
		return cachedValue.Value, nil
	}

	cachedValue, err := doFetch(c, key, readControl)
	return cachedValue.Value, err
}

// Attempt to retrieve an item from the cache, if it exists and hasn't expired.
// Returns somevalue, true if exists or nil, false if it does not.
func getFromCache(c *readcache, key string) (*cacheable, bool) {
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
			c.CacheLock.Unlock()
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
func getReadControl(c *readcache, key string) (readControl *sync.Once, cachedItem *cacheable, gotCachedItem bool) {
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
func doFetch(c *readcache, key string, readControl *sync.Once) (cachedValue *cacheable, err error) {
	fetchedValue := false
	readControl.Do(func() {
		defer func() {
			c.ReadControlsLock.Lock()
			delete(c.ReadControls, key)
			c.ReadControlsLock.Unlock()
		}()

		value, expiresAt, err := c.Getter(key)
		fetchedValue = true
		if err == nil {
			cachedValue = &cacheable{value, expiresAt}
			c.CacheLock.Lock()
			c.Cache[key] = cachedValue
			c.CacheLock.Unlock()
		}
	})

	if !fetchedValue {
		ok := false
		c.CacheLock.RLock()
		cachedValue, ok = c.Cache[key]
		c.CacheLock.RUnlock()

		// An error may have occured during the fetch; if so then retry.
		if !ok {
			err = errors.New(fmt.Sprintf("An error occured during a concurrent fetch for '%s'", key))
		}
	}

	return
}
