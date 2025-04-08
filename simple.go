package gcache

import (
	"context"
	"errors"
	"time"

	"github.com/limpo1989/gcache/internal/pqueue"
)

// SimpleCache has no clear priority for evict cache. It depends on key-value map order.
type SimpleCache[K comparable, V any] struct {
	baseCache[K, V]
	items map[K]*simpleItem[K, V]
}

func newSimpleCache[K comparable, V any](cb *CacheBuilder[K, V]) *SimpleCache[K, V] {
	c := &SimpleCache[K, V]{}
	buildCache(&c.baseCache, cb)
	c.fetch = c.getValue

	c.init()
	return c
}

func (c *SimpleCache[K, V]) init() {
	if c.size <= 0 {
		c.items = make(map[K]*simpleItem[K, V])
	} else {
		c.items = make(map[K]*simpleItem[K, V], c.size)
	}
}

// Set a new key-value pair
func (c *SimpleCache[K, V]) Set(key K, value *V) error {
	return c.SetWithExpire(key, value, c.expiration)
}

// SetWithExpire set a new key-value pair with an expiration time
func (c *SimpleCache[K, V]) SetWithExpire(key K, value *V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setWithExpire(key, c.obtain(value), expiration)
}

func (c *SimpleCache[K, V]) setWithExpire(key K, value *V, expiration time.Duration) error {
	// Check for existing item
	item, ok := c.items[key]
	if ok {
		if nil != c.allocator {
			c.allocator.Free(item.value)
		}
		item.value = value
		item.expiration = c.calcExpiration(expiration)
		if item.elem != nil {
			if item.expiration.IsZero() {
				c.expirations.Remove(item.elem)
				item.elem = nil
			} else {
				c.expirations.Update(item.elem, item.expiration.UnixNano())
			}
		}
	} else {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evict(1)
		}
		item = &simpleItem[K, V]{
			value:      value,
			expiration: c.calcExpiration(expiration),
		}
		if !item.expiration.IsZero() {
			item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
			c.expirations.Push(item.elem)
		}
		c.items[key] = item
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return nil
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *SimpleCache[K, V]) Get(ctx context.Context, key K) (*V, error) {
	if c.allocator != nil {
		panic("Arena enabled please use GetFn")
	}
	v, err := c.getValue(key, false, true, c.autoRenewal)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		return c.getWithLoader(ctx, key)
	}
	return v, err
}

// GetIfPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns ErrKeyNotFound.
func (c *SimpleCache[K, V]) GetIfPresent(ctx context.Context, key K) (*V, error) {
	if c.allocator != nil {
		panic("Arena enabled please use GetIfPresentFn")
	}
	return c.getValue(key, false, true, c.autoRenewal)
}

// GetFn callbacks the value for the specified key if it is present in the cache.
// If the key is not present in the cache and the cache has LoaderFunc,
// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
// If the key is not present in the cache and the cache does not have a LoaderFunc,
// Callback ErrKeyNotFound.
func (c *SimpleCache[K, V]) GetFn(ctx context.Context, key K, fn func(value *V, err error)) {

	v, err := c.getValue(key, false, true, false)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		v, err = c.getWithLoader(ctx, key)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if nil == err {
		v, err = c.getValue(key, false, false, c.autoRenewal)
	}

	fn(v, err)
}

// GetIfPresentFn callbacks the value for the specified key if it is present in the cache.
// Callback ErrKeyNotFound if the key is not present.
func (c *SimpleCache[K, V]) GetIfPresentFn(ctx context.Context, key K, fn func(value *V, err error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.getValue(key, false, false, c.autoRenewal)
	fn(v, err)
}

// Touch updates the expiration time of an existing key.
// If the key is not present in the cache and the cache has LoaderFunc,
// invoke the `LoaderFunc` function and inserts the key-value pair in the cache.
// If the key is not present in the cache and the cache does not have a LoaderFunc,
// return ErrKeyNotFound.
func (c *SimpleCache[K, V]) Touch(ctx context.Context, key K) error {
	_, err := c.getValue(key, false, true, true)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		_, err = c.getWithLoader(ctx, key)
	}
	return err
}

// Compact clear expired items
func (c *SimpleCache[K, V]) Compact() (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var now = c.clock.Now().UnixNano()
	for !c.expirations.IsEmpty() {
		item := c.expirations.Peek()
		if now < item.Priority {
			break
		}
		c.remove(item.Value)
		n++
	}
	return
}

func (c *SimpleCache[K, V]) getValue(key K, onLoad, withLock, renewal bool) (*V, error) {
	if withLock {
		c.mu.Lock()
	}
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(time.Time{}, c.clock) {
			v := item.value
			if renewal && c.expiration > 0 {
				item.expiration = c.calcExpiration(c.expiration)
				if item.elem != nil {
					c.expirations.Update(item.elem, item.expiration.UnixNano())
				} else {
					item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
					c.expirations.Push(item.elem)
				}
			}
			if withLock {
				c.mu.Unlock()
			}
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.remove(key)
	}
	if withLock {
		c.mu.Unlock()
	}
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, ErrKeyNotFound
}

func (c *SimpleCache[K, V]) getWithLoader(ctx context.Context, key K) (*V, error) {
	if c.loaderExpireFunc == nil {
		return nil, ErrKeyNotFound
	}
	return c.load(ctx, key, func(val *V, expiration time.Duration) (*V, error) {
		value := c.obtain(val)
		e := c.setWithExpire(key, value, expiration)
		return value, e
	})
}

func (c *SimpleCache[K, V]) evict(count int) {
	now := c.clock.Now()
	current := 0
	for key, item := range c.items {
		if current >= count {
			return
		}
		if item.expiration.IsZero() || now.After(item.expiration) {
			defer c.remove(key)
			current++
		}
	}
}

// Has checks if key exists in cache
func (c *SimpleCache[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.has(key, time.Time{})
}

func (c *SimpleCache[K, V]) has(key K, now time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now, c.clock)
}

// Remove removes the provided key from the cache.
func (c *SimpleCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *SimpleCache[K, V]) remove(key K) bool {
	item, ok := c.items[key]
	if ok {
		delete(c.items, key)
		if item.elem != nil {
			c.expirations.Remove(item.elem)
		}
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		if nil != c.allocator {
			c.allocator.Free(item.value)
		}

		item.value = nil
		item.elem = nil
		return true
	}
	return false
}

// Keys returns a slice of the keys in the cache.
func (c *SimpleCache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *SimpleCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all key-value pairs from the cache
func (c *SimpleCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if item.elem != nil {
			c.expirations.Remove(item.elem)
		}
		if c.purgeVisitorFunc != nil {
			c.purgeVisitorFunc(key, item.value)
		}
		if nil != c.allocator {
			c.allocator.Free(item.value)
		}

		item.value = nil
		item.elem = nil
	}

	c.init()
}

type simpleItem[K comparable, V any] struct {
	value      *V
	expiration time.Time
	elem       *pqueue.Elem[K]
}

// IsExpired returns boolean value whether this item is expired or not.
func (si *simpleItem[K, V]) IsExpired(now time.Time, clock Clock) bool {
	if si.expiration.IsZero() {
		return false
	}
	if now.IsZero() {
		now = clock.Now()
	}
	return si.expiration.Before(now)
}
