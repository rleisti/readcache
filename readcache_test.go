package readcache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGet_Once_WithNilValue_ShouldReturnNil(t *testing.T) {
	cache := New(func(key string) (interface{}, time.Time, error) { return nil, time.Now().Add(100e9), nil })
	cache.Get("key")
}

func TestGet_Once_WithSomeValue_ShouldReturnValue(t *testing.T) {
	cache := New(func(key string) (interface{}, time.Time, error) { return "foo", time.Now().Add(100e9), nil })
	result, _ := cache.Get("key")
	if result == nil {
		t.Error("Something should have been returned.")
	}
	if result.(string) != "foo" {
		t.Error("Did not get the expected value.")
	}
}

func TestGet_Twice_WithSomeValue_ShouldReturnValue(t *testing.T) {
	cache := New(func(key string) (interface{}, time.Time, error) { return "foo", time.Now().Add(100e9), nil })
	cache.Get("key")
	result, _ := cache.Get("key")
	if result == nil {
		t.Error("Something should have been returned.")
	}
	if result.(string) != "foo" {
		t.Error("Did not get the expected value.")
	}
}

func TestGet_Twice_WithSomeValue_ShouldNotFetchTwice(t *testing.T) {
	fetchCount := 0
	getter := func(key string) (interface{}, time.Time, error) {
		fetchCount++
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)
	cache.Get("key")
	cache.Get("key")
	if fetchCount != 1 {
		t.Errorf("Should have only fetched once, but got %d", fetchCount)
	}
}

func TestGet_Twice_WithExpiration_ShouldFetchTwice(t *testing.T) {
	expiresAt := time.Now().Add(-1)
	fetchCount := 0
	getter := func(key string) (interface{}, time.Time, error) {
		fetchCount++
		return "foo", expiresAt, nil
	}
	cache := New(getter)
	cache.Get("key")
	cache.Get("key")
	if fetchCount != 2 {
		t.Errorf("Should have fetched twice, but got %d", fetchCount)
	}
}

func TestGet_ConcurrentReads_WithLongExpiration_ShouldFetchOncePerKey(t *testing.T) {
	fetchLock := new(sync.Mutex)
	fetchCount := 0
	getter := func(key string) (interface{}, time.Time, error) {
		fetchLock.Lock()
		fetchCount++
		fetchLock.Unlock()
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)

	numKeys := 32
	runConcurrencyTestWithSingleFetch(t, cache, numKeys)
	if fetchCount != numKeys {
		t.Errorf("Should have fetched %d times, but got %d", numKeys, fetchCount)
	}
}

func TestGet_ConcurrentReads_StartingWithExpiredItems_ShouldFetchOncePerKey(t *testing.T) {
	fetchLock := new(sync.Mutex)
	prime := true
	expiresAt := time.Now().Add(-1)
	fetchCount := 0
	getter := func(key string) (interface{}, time.Time, error) {
		if prime {
			return "foo", expiresAt, nil
		} else {
			fetchLock.Lock()
			fetchCount++
			fetchLock.Unlock()
		}
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)

	numKeys := 32
	for i := 0; i < numKeys; i++ {
		cache.Get(fmt.Sprintf("%d", i))
	}
	prime = false

	runConcurrencyTestWithSingleFetch(t, cache, numKeys)
	if fetchCount != numKeys {
		t.Errorf("Should have fetched %d times, but got %d", numKeys, fetchCount)
	}
}

func runConcurrencyTestWithSingleFetch(t *testing.T, cache Cache, numKeys int) {
	numGoroutines := 32
	numFetchesPerGoroutine := numKeys * 4

	quit := make(chan bool)
	for r := 0; r < numGoroutines; r++ {
		seed := r
		go func() {
			for i := 0; i < numFetchesPerGoroutine; i++ {
				keyNum := ((i + 1) * seed) % numKeys
				key := fmt.Sprintf("%d", keyNum)
				cache.Get(key)
			}
			quit <- true
		}()
	}
	for r := 0; r < numGoroutines; r++ {
		<-quit
	}
}
