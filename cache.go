package gcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/limpo1989/arena"
	"github.com/limpo1989/gcache/internal/pqueue"
	"github.com/samber/go-singleflightx"
)

const (
	TypeSimple = "simple"
	TypeLru    = "lru"
	TypeLfu    = "lfu"
	TypeArc    = "arc"
)

var ErrKeyNotFound = errors.New("key not found")

type Cache[K comparable, V any] interface {
	// Set inserts or updates the specified key-value pair.
	Set(key K, value *V) error
	// SetWithExpire inserts or updates the specified key-value pair with an expiration time.
	SetWithExpire(key K, value *V, expiration time.Duration) error
	// Get returns the value for the specified key if it is present in the cache.
	// If the key is not present in the cache and the cache has LoaderFunc,
	// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
	// If the key is not present in the cache and the cache does not have a LoaderFunc,
	// return ErrKeyNotFound.
	Get(ctx context.Context, key K) (*V, error)
	// GetIfPresent returns the value for the specified key if it is present in the cache.
	// Return ErrKeyNotFound if the key is not present.
	GetIfPresent(ctx context.Context, key K) (*V, error)
	// GetFn callbacks the value for the specified key if it is present in the cache.
	// If the key is not present in the cache and the cache has LoaderFunc,
	// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
	// If the key is not present in the cache and the cache does not have a LoaderFunc,
	// Callback ErrKeyNotFound.
	GetFn(ctx context.Context, key K, fn func(value *V, err error))
	// GetIfPresentFn callbacks the value for the specified key if it is present in the cache.
	// Callback ErrKeyNotFound if the key is not present.
	GetIfPresentFn(ctx context.Context, key K, fn func(value *V, err error))
	// Touch updates the expiration time of an existing key.
	// If the key is not present in the cache and the cache has LoaderFunc,
	// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
	// If the key is not present in the cache and the cache does not have a LoaderFunc,
	// return ErrKeyNotFound.
	Touch(ctx context.Context, key K) error
	// Remove removes the specified key from the cache if the key is present.
	// Returns true if the key was present and the key has been deleted.
	Remove(key K) bool
	// Clear removes all key-value pairs from the cache.
	Clear()
	// Keys returns a slice containing all keys in the cache.
	Keys() []K
	// Len returns the number of items in the cache.
	Len() int
	// Has returns true if the key exists in the cache.
	Has(key K) bool
	// Compact clear expired items
	Compact() int

	statsAccessor
}

type RWMutex interface {
	sync.Locker
	RLock()
	RUnlock()
}

type baseCache[K comparable, V any] struct {
	clock            Clock
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       time.Duration
	autoRenewal      bool
	mu               RWMutex
	loadGroup        singleflightx.Group[K, *V]
	fetch            func(key K, onLoad, withLock, renewal bool) (*V, error)
	allocator        *arena.Arena
	expirations      *pqueue.PriorityQueue[K]
	*stats
}

type (
	LoaderFunc[K comparable, V any]       func(context.Context, K) (*V, error)
	LoaderExpireFunc[K comparable, V any] func(context.Context, K) (*V, time.Duration, error)
	EvictedFunc[K comparable, V any]      func(K, *V)
	PurgeVisitorFunc[K comparable, V any] func(K, *V)
	AddedFunc[K comparable, V any]        func(K, *V)
)

type CacheBuilder[K comparable, V any] struct {
	clock            Clock
	tp               string
	size             int
	loaderExpireFunc LoaderExpireFunc[K, V]
	evictedFunc      EvictedFunc[K, V]
	purgeVisitorFunc PurgeVisitorFunc[K, V]
	addedFunc        AddedFunc[K, V]
	expiration       time.Duration
	autoRenewal      bool
	shared           int
	mu               RWMutex
	allocator        *arena.Arena
	stats            *stats
}

func New[K comparable, V any](size int) *CacheBuilder[K, V] {
	return &CacheBuilder[K, V]{
		clock: NewRealClock(),
		tp:    TypeSimple,
		size:  size,
		mu:    new(sync.RWMutex),
		stats: &stats{},
	}
}

func (cb *CacheBuilder[K, V]) Clock(clock Clock) *CacheBuilder[K, V] {
	cb.clock = clock
	return cb
}

// LoaderFunc Set a loader function.
// loaderFunc: create a new value with this function if cached value is expired.
func (cb *CacheBuilder[K, V]) LoaderFunc(loaderFunc LoaderFunc[K, V]) *CacheBuilder[K, V] {
	cb.loaderExpireFunc = func(ctx context.Context, k K) (*V, time.Duration, error) {
		v, err := loaderFunc(ctx, k)
		return v, 0, err
	}
	return cb
}

// LoaderExpireFunc Set a loader function with expiration.
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
	return cb.EvictType(TypeSimple)
}

func (cb *CacheBuilder[K, V]) LRU() *CacheBuilder[K, V] {
	return cb.EvictType(TypeLru)
}

func (cb *CacheBuilder[K, V]) LFU() *CacheBuilder[K, V] {
	return cb.EvictType(TypeLfu)
}

func (cb *CacheBuilder[K, V]) ARC() *CacheBuilder[K, V] {
	return cb.EvictType(TypeArc)
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

func (cb *CacheBuilder[K, V]) Expiration(expiration time.Duration) *CacheBuilder[K, V] {
	cb.expiration = expiration
	return cb
}

func (cb *CacheBuilder[K, V]) AutoRenewal(autoRenewal bool) *CacheBuilder[K, V] {
	cb.autoRenewal = autoRenewal
	return cb
}

func (cb *CacheBuilder[K, V]) NopLocker() *CacheBuilder[K, V] {
	cb.mu = nopRWMutex{}
	return cb
}

func (cb *CacheBuilder[K, V]) Shard(shared int) *CacheBuilder[K, V] {
	cb.shared = shared
	if shared > 1 {
		cb.size = cb.size/shared + 1
	}
	return cb
}

func (cb *CacheBuilder[K, V]) Arena(opts ...arena.Option) *CacheBuilder[K, V] {
	cb.allocator = arena.NewArena(opts...)
	return cb
}

func (cb *CacheBuilder[K, V]) Build() Cache[K, V] {
	if cb.size <= 0 && cb.tp != TypeSimple {
		panic("gcache: Cache size <= 0")
	}

	if cb.shared <= 1 {
		return cb.build()
	}

	caches := make([]Cache[K, V], cb.shared)
	for i := 0; i < cb.shared; i++ {
		caches[i] = cb.build()
	}
	return newShardCache[K, V](cb.stats, caches)
}

func (cb *CacheBuilder[K, V]) build() Cache[K, V] {
	switch cb.tp {
	case TypeSimple:
		return newSimpleCache[K, V](cb)
	case TypeLru:
		return newLRUCache[K, V](cb)
	case TypeLfu:
		return newLFUCache[K, V](cb)
	case TypeArc:
		return newARC[K, V](cb)
	default:
		panic("gcache: Unknown type " + cb.tp)
	}
}

func buildCache[K comparable, V any](c *baseCache[K, V], cb *CacheBuilder[K, V]) {
	c.clock = cb.clock
	c.size = cb.size
	c.mu = cb.mu
	c.loaderExpireFunc = cb.loaderExpireFunc
	c.expiration = cb.expiration
	c.autoRenewal = cb.autoRenewal
	c.addedFunc = cb.addedFunc
	c.evictedFunc = cb.evictedFunc
	c.purgeVisitorFunc = cb.purgeVisitorFunc
	c.allocator = cb.allocator
	c.stats = cb.stats
	if c.expiration > 0 {
		c.expirations = pqueue.NewPriorityQueue[K](c.size)
	} else {
		c.expirations = pqueue.NewPriorityQueue[K](max(8, c.size/10))
	}
}

func (c *baseCache[K, V]) calcExpiration(expiration time.Duration) time.Time {
	if expiration == 0 {
		return time.Time{}
	}
	return c.clock.Now().Add(expiration)
}

// load a new value using by specified key.
func (c *baseCache[K, V]) load(ctx context.Context, key K, withLock bool, cb func(*V, time.Duration) (*V, error)) (*V, error) {
	v, err, _ := c.loadGroup.Do(key, func() (v *V, e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("loader panics: %v", r)
			}
		}()

		if withLock {
			c.mu.Lock()
			defer c.mu.Unlock()
		}

		if value, err := c.fetch(key, true, false, true); err == nil {
			return value, nil
		}

		value, expire, err := c.loaderExpireFunc(ctx, key)
		if nil != err {
			return nil, err
		}

		if 0 == expire {
			expire = c.expiration
		}
		return cb(value, expire)
	})
	return v, err
}

func (c *baseCache[K, V]) obtain(v *V) *V {
	if nil == c.allocator {
		return v
	}
	return arena.DeepCopy[V](c.allocator, *v)
}

type nopRWMutex struct{}

func (n nopRWMutex) Lock() {}

func (n nopRWMutex) Unlock() {}

func (n nopRWMutex) RLock() {}

func (n nopRWMutex) RUnlock() {}
