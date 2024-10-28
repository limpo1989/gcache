package gcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	TYPE_SIMPLE = "simple"
	TYPE_LRU    = "lru"
	TYPE_LFU    = "lfu"
	TYPE_ARC    = "arc"
)

var KeyNotFoundError = errors.New("Key not found.")

type Cache[K comparable, V any] interface {
	// Set inserts or updates the specified key-value pair.
	Set(key K, value V) error
	// SetWithExpire inserts or updates the specified key-value pair with an expiration time.
	SetWithExpire(key K, value V, expiration time.Duration) error
	// Get returns the value for the specified key if it is present in the cache.
	// If the key is not present in the cache and the cache has LoaderFunc,
	// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
	// If the key is not present in the cache and the cache does not have a LoaderFunc,
	// return KeyNotFoundError.
	Get(ctx context.Context, key K) (V, error)
	// GetIFPresent returns the value for the specified key if it is present in the cache.
	// Return KeyNotFoundError if the key is not present.
	GetIFPresent(ctx context.Context, key K) (V, error)
	// GetAll returns a map containing all key-value pairs in the cache.
	GetALL(checkExpired bool) map[K]V
	get(key K, onLoad bool) (V, error)
	// Remove removes the specified key from the cache if the key is present.
	// Returns true if the key was present and the key has been deleted.
	Remove(key K) bool
	// Purge removes all key-value pairs from the cache.
	Purge()
	// Keys returns a slice containing all keys in the cache.
	Keys(checkExpired bool) []K
	// Len returns the number of items in the cache.
	Len(checkExpired bool) int
	// Has returns true if the key exists in the cache.
	Has(key K) bool
	// Compact clear expired items
	Compact() int

	statsAccessor
}

type baseCache[K comparable, V any] struct {
	clock            Clock
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       *time.Duration
	autoRenewal      bool
	mu               sync.RWMutex
	loadGroup        Group[K, V]
	*stats
}

type (
	LoaderFunc[K comparable, V any]       func(context.Context, K) (V, error)
	LoaderExpireFunc[K comparable, V any] func(context.Context, K) (V, *time.Duration, error)
	EvictedFunc[K comparable, V any]      func(K, V)
	PurgeVisitorFunc[K comparable, V any] func(K, V)
	AddedFunc[K comparable, V any]        func(K, V)
	KeyHasherFunc[K comparable]           func(K) uintptr
)

type CacheBuilder[K comparable, V any] struct {
	clock            Clock
	tp               string
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       *time.Duration
	autoRenewal      bool
	shared           int
	hasher           KeyHasherFunc[K]
	stats            *stats
}

func New[K comparable, V any](size int) *CacheBuilder[K, V] {
	return &CacheBuilder[K, V]{
		clock: NewRealClock(),
		tp:    TYPE_SIMPLE,
		size:  size,
		stats: &stats{},
	}
}

func (cb *CacheBuilder[K, V]) Clock(clock Clock) *CacheBuilder[K, V] {
	cb.clock = clock
	return cb
}

// Set a loader function.
// loaderFunc: create a new value with this function if cached value is expired.
func (cb *CacheBuilder[K, V]) LoaderFunc(loaderFunc LoaderFunc[K, V]) *CacheBuilder[K, V] {
	cb.loaderExpireFunc = func(ctx context.Context, k K) (V, *time.Duration, error) {
		v, err := loaderFunc(ctx, k)
		return v, nil, err
	}
	return cb
}

// Set a loader function with expiration.
// loaderExpireFunc: create a new value with this function if cached value is expired.
// If nil returned instead of time.Duration from loaderExpireFunc than value will never expire.
func (cb *CacheBuilder[K, V]) LoaderExpireFunc(loaderExpireFunc LoaderExpireFunc[K, V]) *CacheBuilder[K, V] {
	cb.loaderExpireFunc = loaderExpireFunc
	return cb
}

func (cb *CacheBuilder[K, V]) EvictType(tp string) *CacheBuilder[K, V] {
	cb.tp = tp
	return cb
}

func (cb *CacheBuilder[K, V]) Simple() *CacheBuilder[K, V] {
	return cb.EvictType(TYPE_SIMPLE)
}

func (cb *CacheBuilder[K, V]) LRU() *CacheBuilder[K, V] {
	return cb.EvictType(TYPE_LRU)
}

func (cb *CacheBuilder[K, V]) LFU() *CacheBuilder[K, V] {
	return cb.EvictType(TYPE_LFU)
}

func (cb *CacheBuilder[K, V]) ARC() *CacheBuilder[K, V] {
	return cb.EvictType(TYPE_ARC)
}

func (cb *CacheBuilder[K, V]) EvictedFunc(evictedFunc EvictedFunc[K, V]) *CacheBuilder[K, V] {
	cb.evictedFunc = evictedFunc
	return cb
}

func (cb *CacheBuilder[K, V]) PurgeVisitorFunc(purgeVisitorFunc PurgeVisitorFunc[K, V]) *CacheBuilder[K, V] {
	cb.purgeVisitorFunc = purgeVisitorFunc
	return cb
}

func (cb *CacheBuilder[K, V]) AddedFunc(addedFunc AddedFunc[K, V]) *CacheBuilder[K, V] {
	cb.addedFunc = addedFunc
	return cb
}

func (cb *CacheBuilder[K, V]) Expiration(expiration time.Duration, autoRenewal bool) *CacheBuilder[K, V] {
	cb.expiration = &expiration
	cb.autoRenewal = autoRenewal
	return cb
}

func (cb *CacheBuilder[K, V]) Shard(shared int, hasher KeyHasherFunc[K]) *CacheBuilder[K, V] {
	cb.shared = shared
	cb.hasher = hasher
	if shared > 1 {
		cb.size = cb.size/shared + 1
	}
	return cb
}

func (cb *CacheBuilder[K, V]) Build() Cache[K, V] {
	if cb.size <= 0 && cb.tp != TYPE_SIMPLE {
		panic("gcache: Cache size <= 0")
	}

	if cb.shared <= 1 {
		return cb.build()
	}

	caches := make([]Cache[K, V], cb.shared)
	for i := 0; i < cb.shared; i++ {
		caches[i] = cb.build()
	}
	return newShardCache[K, V](cb.stats, caches, cb.hasher)
}

func (cb *CacheBuilder[K, V]) build() Cache[K, V] {
	switch cb.tp {
	case TYPE_SIMPLE:
		return newSimpleCache[K, V](cb)
	case TYPE_LRU:
		return newLRUCache[K, V](cb)
	case TYPE_LFU:
		return newLFUCache[K, V](cb)
	case TYPE_ARC:
		return newARC[K, V](cb)
	default:
		panic("gcache: Unknown type " + cb.tp)
	}
}

func buildCache[K comparable, V any](c *baseCache[K, V], cb *CacheBuilder[K, V]) {
	c.clock = cb.clock
	c.size = cb.size
	c.loaderExpireFunc = cb.loaderExpireFunc
	c.expiration = cb.expiration
	c.autoRenewal = cb.autoRenewal
	c.addedFunc = cb.addedFunc
	c.evictedFunc = cb.evictedFunc
	c.purgeVisitorFunc = cb.purgeVisitorFunc
	c.stats = cb.stats
}

// load a new value using by specified key.
func (c *baseCache[K, V]) load(ctx context.Context, key K, cb func(V, *time.Duration, error) (V, error), isWait bool) (V, bool, error) {
	v, called, err := c.loadGroup.Do(key, func() (v V, e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("Loader panics: %v", r)
			}
		}()
		return cb(c.loaderExpireFunc(ctx, key))
	}, isWait)
	return v, called, err
}
