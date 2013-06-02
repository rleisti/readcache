Package readcache implements a read-through cache. Items are fetched by a user-provided function.

This cache is designed for concurrency, and guarantees that concurrent access to an item only results in a single fetch.

# Sample Usage

	package main

	import (
		"github.com/rleisti/readcache"
		"fmt"
		"time"
	)

	func fetch(key string) (interface {}, time.Time, error) {
		// retrieve the value for the given key
		value := "value"
		return value, time.Now().Add(60e9), nil
	}

	func main() {
		cache := readcache.New(fetch)
		if value, err := cache.Get("key"); err == nil {
			fmt.Printf("key = %s\n", value)
		}
	}

