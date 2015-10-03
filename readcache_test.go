package readcache

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGet_Once_WithNilValue_ShouldReturnNil(t *testing.T) {
	cache := New(newGetter(nil, 100e9))
	cache.Get("key")
}

func TestGet_Once_WithSomeValue_ShouldReturnValue(t *testing.T) {
	cache := New(newGetter("foo", 100e9))
	result, _ := cache.Get("key")
	if result == nil {
		t.Error("Something should have been returned.")
	}
	if result.(string) != "foo" {
		t.Error("Did not get the expected value.")
	}
}

func TestGet_Twice_WithSomeValue_ShouldReturnValue(t *testing.T) {
	cache := New(newGetter("foo", 100e9))
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
	runConcurrencyTest(cache, numKeys, numKeys)
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
		}

		fetchLock.Lock()
		fetchCount++
		fetchLock.Unlock()
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)

	numKeys := 32
	for i := 0; i < numKeys; i++ {
		cache.Get(fmt.Sprintf("%d", i))
	}
	prime = false

	runConcurrencyTest(cache, numKeys, numKeys)
	if fetchCount != numKeys {
		t.Errorf("Should have fetched %d times, but got %d", numKeys, fetchCount)
	}
}

func TestGet_ErrorInGetter_ShouldReturnError(t *testing.T) {
	getter := func(key string) (interface{}, time.Time, error) {
		return nil, time.Now(), errors.New("Error message")
	}
	cache := New(getter)
	_, err := cache.Get("key")
	if err == nil {
		t.Error("An error should have been returned")
	} else if err.Error() != "Error message" {
		t.Errorf("Expected 'Error message' but got '%s'", err.Error())
	}
}

func TestGet_ErrorInGetter_ConcurrentReads_ShouldReturnError(t *testing.T) {
	getter := func(key string) (interface{}, time.Time, error) {
		return nil, time.Now(), errors.New("Error message")
	}
	cache := New(getter)
	quit := make(chan bool)
	for r := 0; r < 32; r++ {
		seed := r
		go func() {
			for i := 0; i < 2048; i++ {
				keyNum := ((i + 1) * seed) % 512
				key := fmt.Sprintf("%d", keyNum)
				_, err := cache.Get(key)
				if err == nil {
					t.Error("Get did not return an error")
				} else if err.Error() != "Error message" {
					t.Errorf("Unexpected error result: %s", err.Error())
				}
			}
			quit <- true
		}()
	}
	for r := 0; r < 32; r++ {
		<-quit
	}
}

func TestGet_WithPurgeRules_ShouldPurgeOldEntries(t *testing.T) {
	fetchCount := 0
	getter := func(key string) (interface{}, time.Time, error) {
		fetchCount++
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)
	cache.SetPurgeAt(3)
	cache.SetPurgeTo(1)

	cache.Get("1")
	cache.Get("2")
	cache.Get("1")
	cache.Get("2")
	if fetchCount != 2 {
		t.Errorf("Expected fetchCount = 2 but was %d", fetchCount)
	}
	cache.Get("3") // {1, 2, 3} -> Purge -> {3}
	cache.Get("3")
	if fetchCount != 3 {
		t.Errorf("Expected fetchCount = 3 but was %d", fetchCount)
	}
	cache.Get("1")
	cache.Get("2") // {1, 2, 3} -> Purge -> {2}
	if fetchCount != 5 {
		t.Errorf("Expected fetchCount = 5 but was %d", fetchCount)
	}
	cache.Get("3")
	if fetchCount != 6 {
		t.Errorf("Expected fetchCount = 6 but was %d", fetchCount)
	}
	cache.Get("2")
	if fetchCount != 6 {
		t.Errorf("Expected fetchCount = 6 but was %d", fetchCount)
	}
}

func BenchmarkGet_Concurrent_Performance(t *testing.B) {
	getter := func(key string) (interface{}, time.Time, error) {
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)
	runConcurrencyTest(cache, 8, t.N)
}

func BenchmarkGet_Concurrent_Purge_Performance(t *testing.B) {
	getter := func(key string) (interface{}, time.Time, error) {
		return "foo", time.Now().Add(100e9), nil
	}
	cache := New(getter)
	cache.SetPurgeAt(200)
	cache.SetPurgeTo(100)
	runConcurrencyTest(cache, 8, t.N)
}

func runConcurrencyTest(cache Cache, numGoroutines int, numFetches int) {
	numFetchesPerGoroutine := numFetches / numGoroutines
	remainingFetches := numFetches % numGoroutines
	numKeys := numFetches

	quit := make(chan bool)
	for r := 0; r < numGoroutines; r++ {
		seed := r
		go func() {
			var numFetchesForThis = numFetchesPerGoroutine
			if r == (numGoroutines - 1) {
				numFetchesForThis += remainingFetches
			}
			for i := 0; i < numFetchesForThis; i++ {
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

func newGetter(item interface{}, expirationDelta time.Duration) func(string) (interface{}, time.Time, error) {
	return func(key string) (interface{}, time.Time, error) {
		return item, time.Now().Add(expirationDelta), nil
	}
}
