package readcache

import (
	"testing"
	"time"
)

func TestGet_Once_WithNilValue_ShouldReturnNil(t *testing.T) {
	item := newItem(nil)
	cache := New(func(key string) Cacheable { return item })
	cache.Get("key")
}

func TestGet_Once_WithSomeValue_ShouldReturnValue(t *testing.T) {
	item := newItem("foo")
	cache := New(func(key string) Cacheable { return item })
	result := cache.Get("key")
	if result == nil {
		t.Error("Something should have been returned.")
	}
	if result.(string) != "foo" {
		t.Error("Did not get the expected value.")
	}
}

func TestGet_Twice_WithSomeValue_ShouldReturnValue(t *testing.T) {
	item := newItem("foo")
	cache := New(func(key string) Cacheable { return item })
	cache.Get("key")
	result := cache.Get("key")
	if result == nil {
		t.Error("Something should have been returned.")
	}
	if result.(string) != "foo" {
		t.Error("Did not get the expected value.")
	}
}

func TestGet_Twice_WithSomeValue_ShouldNotFetchTwice(t *testing.T) {
	item := newItem("foo")
	fetchCount := 0
	getter := func(key string) Cacheable {
		fetchCount++
		return item
	}
	cache := New(getter)
	cache.Get("key")
	cache.Get("key")
	if fetchCount != 1 {
		t.Errorf("Should have only fetched once, but got %d", fetchCount)
	}
}

func TestGet_Twice_WithExpiration_ShouldFetchTwice(t *testing.T) {
	item := Cacheable{Value: "foo", ExpiresAt: time.Now().Add(-1)}
	fetchCount := 0
	getter := func(key string) Cacheable {
		fetchCount++
		return item
	}
	cache := New(getter)
	cache.Get("key")
	cache.Get("key")
	if fetchCount != 2 {
		t.Errorf("Should have fetched twice, but got %d", fetchCount)
	}
}

// Constructs a new item with the given value, which is not expected to expire
// soon.
func newItem(value interface{}) Cacheable {
	return Cacheable{Value: value, ExpiresAt: time.Now().Add(100e9)}
}
