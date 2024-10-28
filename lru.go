package gcache

import (
	"container/list"
	"context"
	"time"
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

	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *LRUCache[K, V]) init() {
	c.evictList = list.New()
	c.items = make(map[K]*list.Element, c.size+1)
}

func (c *LRUCache[K, V]) set(key K, value V) (*lruItem[K, V], error) {
	// Check for existing item
	var item *lruItem[K, V]
	if it, ok := c.items[key]; ok {
		c.evictList.MoveToFront(it)
		item = it.Value.(*lruItem[K, V])
		item.value = value
	} else {
		// Verify size not exceeded
		if c.evictList.Len() >= c.size {
			c.evict(1)
		}
		item = &lruItem[K, V]{
			clock: c.clock,
			key:   key,
			value: value,
		}
		c.items[key] = c.evictList.PushFront(item)
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return item, nil
}

// set a new key-value pair
func (c *LRUCache[K, V]) Set(key K, value V) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// Set a new key-value pair with an expiration time
func (c *LRUCache[K, V]) SetWithExpire(key K, value V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.expiration = &t
	return nil
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *LRUCache[K, V]) Get(ctx context.Context, key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(ctx, key, true)
	}
	return v, err
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *LRUCache[K, V]) GetIFPresent(ctx context.Context, key K) (V, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(ctx, key, false)
	}
	return v, err
}

// Compact clear expired items
func (c *LRUCache[K, V]) Compact() (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, item := range c.items {
		it := item.Value.(*lruItem[K, V])
		if it.IsExpired(nil) {
			c.removeElement(item)
			n++
		}
	}
	return
}

func (c *LRUCache[K, V]) get(key K, onLoad bool) (V, error) {
	return c.getValue(key, onLoad)
}

func (c *LRUCache[K, V]) getValue(key K, onLoad bool) (V, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		it := item.Value.(*lruItem[K, V])
		if !it.IsExpired(nil) {
			c.evictList.MoveToFront(item)
			v := it.value
			if c.autoRenewal && c.expiration != nil {
				t := it.clock.Now().Add(*c.expiration)
				it.expiration = &t
			}
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.removeElement(item)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	var zero V
	return zero, KeyNotFoundError
}

func (c *LRUCache[K, V]) getWithLoader(ctx context.Context, key K, isWait bool) (V, error) {
	var zero V
	if c.loaderExpireFunc == nil {
		return zero, KeyNotFoundError
	}
	value, _, err := c.load(ctx, key, func(v V, expiration *time.Duration, e error) (V, error) {
		if e != nil {
			return zero, e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.set(key, v)
		if err != nil {
			return zero, err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.expiration = &t
		}
		return v, nil
	}, isWait)
	return value, err
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
	now := time.Now()
	return c.has(key, &now)
}

func (c *LRUCache[K, V]) has(key K, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.Value.(*lruItem[K, V]).IsExpired(now)
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
	c.evictList.Remove(e)
	entry := e.Value.(*lruItem[K, V])
	delete(c.items, entry.key)
	if c.evictedFunc != nil {
		entry := e.Value.(*lruItem[K, V])
		c.evictedFunc(entry.key, entry.value)
	}
}

func (c *LRUCache[K, V]) keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, len(c.items))
	var i = 0
	for k := range c.items {
		keys[i] = k
		i++
	}
	return keys
}

// GetALL returns all key-value pairs in the cache.
func (c *LRUCache[K, V]) GetALL(checkExpired bool) map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			items[k] = item.Value.(*lruItem[K, V]).value
		}
	}
	return items
}

// Keys returns a slice of the keys in the cache.
func (c *LRUCache[K, V]) Keys(checkExpired bool) []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	now := time.Now()
	for k := range c.items {
		if !checkExpired || c.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *LRUCache[K, V]) Len(checkExpired bool) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !checkExpired {
		return len(c.items)
	}
	var length int
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
			length++
		}
	}
	return length
}

// Completely clear the cache
func (c *LRUCache[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			it := item.Value.(*lruItem[K, V])
			v := it.value
			c.purgeVisitorFunc(key, v)
		}
	}

	c.init()
}

type lruItem[K comparable, V any] struct {
	clock      Clock
	key        K
	value      V
	expiration *time.Time
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *lruItem[K, V]) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}
