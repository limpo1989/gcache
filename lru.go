package gcache

import (
	"container/list"
	"context"
	"errors"
	"time"

	"github.com/limpo1989/gcache/internal/pqueue"
)

// Discards the least recently used items first.
type LRUCache[K comparable, V any] struct {
	baseCache[K, V]
	items     map[K]*list.Element
	evictList *list.List
}

func newLRUCache[K comparable, V any](cb *CacheBuilder[K, V]) *LRUCache[K, V] {
	c := &LRUCache[K, V]{}
	buildCache(&c.baseCache, cb)
	c.fetch = c.getValue

	c.init()
	return c
}

func (c *LRUCache[K, V]) init() {
	c.evictList = list.New()
	c.items = make(map[K]*list.Element, c.size+1)
}

func (c *LRUCache[K, V]) setWithExpire(key K, value *V, expiration time.Duration) error {
	// Check for existing item
	var item *lruItem[K, V]
	if it, ok := c.items[key]; ok {
		c.evictList.MoveToFront(it)
		item = it.Value.(*lruItem[K, V])
		if nil != c.allocator {
			c.allocator.Free(item.value)
		}
		item.value = value
		item.expiration = c.calcExpiration(expiration)
		if nil != item.elem {
			if item.expiration.IsZero() {
				c.expirations.Remove(item.elem)
				item.elem = nil
			} else {
				c.expirations.Update(item.elem, item.expiration.UnixNano())
			}
		}
	} else {
		// Verify size not exceeded
		if c.evictList.Len() >= c.size {
			c.evict(1)
		}
		item = &lruItem[K, V]{
			key:        key,
			value:      value,
			expiration: c.calcExpiration(expiration),
		}
		if !item.expiration.IsZero() {
			item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
			c.expirations.Push(item.elem)
		}
		c.items[key] = c.evictList.PushFront(item)
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return nil
}

// Set a new key-value pair
func (c *LRUCache[K, V]) Set(key K, value *V) error {
	return c.SetWithExpire(key, value, c.expiration)
}

// SetWithExpire a new key-value pair with an expiration time
func (c *LRUCache[K, V]) SetWithExpire(key K, value *V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setWithExpire(key, c.obtain(value), expiration)
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *LRUCache[K, V]) Get(ctx context.Context, key K) (*V, error) {
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
func (c *LRUCache[K, V]) GetIfPresent(ctx context.Context, key K) (*V, error) {
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
func (c *LRUCache[K, V]) GetFn(ctx context.Context, key K, fn func(value *V, err error)) {
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
func (c *LRUCache[K, V]) GetIfPresentFn(ctx context.Context, key K, fn func(value *V, err error)) {
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
func (c *LRUCache[K, V]) Touch(ctx context.Context, key K) error {
	_, err := c.getValue(key, false, true, true)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		_, err = c.getWithLoader(ctx, key)
	}
	return err
}

// Compact clear expired items
func (c *LRUCache[K, V]) Compact() (n int) {
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

func (c *LRUCache[K, V]) getValue(key K, onLoad, withLock, renewal bool) (*V, error) {
	if withLock {
		c.mu.Lock()
	}
	item, ok := c.items[key]
	if ok {
		it := item.Value.(*lruItem[K, V])
		if !it.IsExpired(time.Time{}, c.clock) {
			c.evictList.MoveToFront(item)
			v := it.value
			if renewal && c.expiration > 0 {
				it.expiration = c.calcExpiration(c.expiration)
				if it.elem != nil {
					c.expirations.Update(it.elem, it.expiration.UnixNano())
				} else {
					it.elem = &pqueue.Elem[K]{Value: key, Priority: it.expiration.UnixNano()}
					c.expirations.Push(it.elem)
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
		c.removeElement(item)
	}
	if withLock {
		c.mu.Unlock()
	}
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, ErrKeyNotFound
}

func (c *LRUCache[K, V]) getWithLoader(ctx context.Context, key K) (*V, error) {
	if c.loaderExpireFunc == nil {
		return nil, ErrKeyNotFound
	}
	return c.load(ctx, key, func(val *V, expiration time.Duration) (*V, error) {
		value := c.obtain(val)
		e := c.setWithExpire(key, value, expiration)
		return value, e
	})
}

// evict removes the oldest item from the cache.
func (c *LRUCache[K, V]) evict(count int) {
	for i := 0; i < count; i++ {
		ent := c.evictList.Back()
		if ent == nil {
			return
		} else {
			c.removeElement(ent)
		}
	}
}

// Has checks if key exists in cache
func (c *LRUCache[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.has(key, time.Time{})
}

func (c *LRUCache[K, V]) has(key K, now time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	entry := item.Value.(*lruItem[K, V])
	return !entry.IsExpired(now, c.clock)
}

// Remove removes the provided key from the cache.
func (c *LRUCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *LRUCache[K, V]) remove(key K) bool {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

func (c *LRUCache[K, V]) removeElement(e *list.Element) {
	item := e.Value.(*lruItem[K, V])
	c.evictList.Remove(e)
	delete(c.items, item.key)
	if item.elem != nil {
		c.expirations.Remove(item.elem)
	}
	if c.evictedFunc != nil {
		c.evictedFunc(item.key, item.value)
	}
	if nil != c.allocator {
		c.allocator.Free(item.value)
	}
	item.value = nil
	item.elem = nil
}

// Keys returns a slice of the keys in the cache.
func (c *LRUCache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *LRUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all key-value pairs from the cache
func (c *LRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		it := item.Value.(*lruItem[K, V])
		v := it.value
		if it.elem != nil {
			c.expirations.Remove(it.elem)
		}
		if c.purgeVisitorFunc != nil {
			c.purgeVisitorFunc(key, v)
		}

		if nil != c.allocator {
			c.allocator.Free(v)
		}
		it.value = nil
		it.elem = nil
	}

	c.init()
}

type lruItem[K comparable, V any] struct {
	key        K
	value      *V
	expiration time.Time
	elem       *pqueue.Elem[K]
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *lruItem[K, V]) IsExpired(now time.Time, clock Clock) bool {
	if it.expiration.IsZero() {
		return false
	}
	if now.IsZero() {
		now = clock.Now()
	}
	return it.expiration.Before(now)
}
