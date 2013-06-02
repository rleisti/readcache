/*
Package readcache implements a read-through cache. Items are fetched by a user-provided function.
*/
package readcache

import (
	"sync"
	"time"
)

// Type Cache defines a read-through cache.
type Cache interface {
	// Retrieve an item from the cache if available, or from a
	// backing source if it is not.
	// May return an error instead, if the item cannot be fetched.
	Get(key string) (interface{}, error)
}

// Constructs a new cache.  The item fetcher may return an item of type interface {} with an
// expiration time, or it may return an error.  If an error is returned, then all other return values are ignored.
func New(getter func(string) (interface{}, time.Time, error)) Cache {
	return &readcache{getter, make(map[string]*cacheable), make(map[string]*readControl), new(sync.RWMutex), new(sync.RWMutex)}
}

// Type cacheable is something that may be stored in a cache
type cacheable struct {
	// The item in the cache
	Value interface{}

	// The time at which this item should expire from the cache.
	ExpiresAt time.Time
}

// Type readControl is a mechanism for controlling concurrent fetches
type readControl struct {
	Controller *sync.Once
	Result     *cacheable
	Error      error
}

// Type readcache implements the Cache interface
type readcache struct {
	// The fetcher of items
	Getter func(string) (interface{}, time.Time, error)

	// The cache of items
	Cache map[string]*cacheable

	// Controls read of items from the getter; prevents multiple concurrent reads of the same item.
	ReadControls map[string]*readControl

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
	if cachedValue != nil {
		return cachedValue.Value, err
	} else {
		return nil, err
	}
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
func getReadControl(c *readcache, key string) (control *readControl, cachedItem *cacheable, gotCachedItem bool) {
	gotCachedItem = false

	c.ReadControlsLock.RLock()
	control, ok := c.ReadControls[key]
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

		control, ok = c.ReadControls[key]
		if !ok {
			control = &readControl{new(sync.Once), nil, nil}
			c.ReadControls[key] = control
		}
		c.ReadControlsLock.Unlock()
	}

	return
}

// Use the given read control to fetch a value and store it in the cache.
// The read control may prevent this goroutine from fetching the value if
// some other routine gets to it first.  In either case, the resulting
// fetched value is returned.
func doFetch(c *readcache, key string, readControl *readControl) (cachedValue *cacheable, err error) {
	readControl.Controller.Do(func() {
		defer func() {
			c.ReadControlsLock.Lock()
			delete(c.ReadControls, key)
			c.ReadControlsLock.Unlock()
		}()

		var value interface{}
		var expiresAt time.Time
		value, expiresAt, err = c.Getter(key)
		if err == nil {
			cachedValue = &cacheable{value, expiresAt}
			readControl.Result = cachedValue
			c.CacheLock.Lock()
			c.Cache[key] = cachedValue
			c.CacheLock.Unlock()
		} else {
			readControl.Error = err
		}
	})

	cachedValue = readControl.Result
	err = readControl.Error

	return
}
