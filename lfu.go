package gcache

import (
	"container/list"
	"context"
	"errors"
	"time"

	"github.com/limpo1989/gcache/internal/pqueue"
)

// Discards the least frequently used items first.
type LFUCache[K comparable, V any] struct {
	baseCache[K, V]
	items    map[K]*lfuItem[K, V]
	freqList *list.List // list for freqEntry
}

type lfuItem[K comparable, V any] struct {
	key         K
	value       *V
	freqElement *list.Element
	expiration  time.Time
	elem        *pqueue.Elem[K]
}

type freqEntry[K comparable, V any] struct {
	freq  uint
	items map[*lfuItem[K, V]]struct{}
}

func newLFUCache[K comparable, V any](cb *CacheBuilder[K, V]) *LFUCache[K, V] {
	c := &LFUCache[K, V]{}
	buildCache(&c.baseCache, cb)
	c.fetch = c.getValue

	c.init()
	return c
}

func (c *LFUCache[K, V]) init() {
	c.freqList = list.New()
	c.items = make(map[K]*lfuItem[K, V], c.size)
	c.freqList.PushFront(&freqEntry[K, V]{
		freq:  0,
		items: make(map[*lfuItem[K, V]]struct{}),
	})
}

// Set a new key-value pair
func (c *LFUCache[K, V]) Set(key K, value *V) error {
	return c.SetWithExpire(key, value, c.expiration)
}

// SetWithExpire a new key-value pair with an expiration time
func (c *LFUCache[K, V]) SetWithExpire(key K, value *V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setWithExpire(key, c.obtain(value), expiration)
}

func (c *LFUCache[K, V]) setWithExpire(key K, value *V, expiration time.Duration) error {
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
		if len(c.items) >= c.size {
			c.evict(1)
		}
		item = &lfuItem[K, V]{
			key:         key,
			value:       value,
			freqElement: nil,
			expiration:  c.calcExpiration(expiration),
		}
		if !item.expiration.IsZero() {
			item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
			c.expirations.Push(item.elem)
		}
		el := c.freqList.Front()
		fe := el.Value.(*freqEntry[K, V])
		fe.items[item] = struct{}{}

		item.freqElement = el
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
func (c *LFUCache[K, V]) Get(ctx context.Context, key K) (*V, error) {
	if c.allocator != nil {
		panic("Arena enabled please use GetFn")
	}
	v, err := c.getValue(key, false, true, c.autoRenewal)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		return c.getWithLoader(ctx, key, true)
	}
	return v, err
}

// GetIfPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns ErrKeyNotFound.
func (c *LFUCache[K, V]) GetIfPresent(ctx context.Context, key K) (*V, error) {
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
func (c *LFUCache[K, V]) GetFn(ctx context.Context, key K, fn func(value *V, err error)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, err := c.getValue(key, false, false, c.autoRenewal)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		v, err = c.getWithLoader(ctx, key, false)
	}

	fn(v, err)
}

// GetIfPresentFn callbacks the value for the specified key if it is present in the cache.
// Callback ErrKeyNotFound if the key is not present.
func (c *LFUCache[K, V]) GetIfPresentFn(ctx context.Context, key K, fn func(value *V, err error)) {
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
func (c *LFUCache[K, V]) Touch(ctx context.Context, key K) error {
	_, err := c.getValue(key, false, true, true)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		_, err = c.getWithLoader(ctx, key, true)
	}
	return err
}

// Compact clear expired items
func (c *LFUCache[K, V]) Compact() (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now().UnixNano()
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

func (c *LFUCache[K, V]) getValue(key K, onLoad, withLock, renewal bool) (*V, error) {
	if withLock {
		c.mu.Lock()
	}

	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(time.Time{}, c.clock) {
			c.increment(item)
			v := item.value
			if renewal && c.expiration != 0 {
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
		c.removeItem(item)
	}
	if withLock {
		c.mu.Unlock()
	}
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, ErrKeyNotFound
}

func (c *LFUCache[K, V]) getWithLoader(ctx context.Context, key K, withLock bool) (*V, error) {
	if c.loaderExpireFunc == nil {
		return nil, ErrKeyNotFound
	}
	return c.load(ctx, key, withLock, func(val *V, expiration time.Duration) (*V, error) {
		value := c.obtain(val)
		e := c.setWithExpire(key, value, expiration)
		return value, e
	})
}

func (c *LFUCache[K, V]) increment(item *lfuItem[K, V]) {
	currentFreqElement := item.freqElement
	currentFreqEntry := currentFreqElement.Value.(*freqEntry[K, V])
	nextFreq := currentFreqEntry.freq + 1
	delete(currentFreqEntry.items, item)

	// a boolean whether reuse the empty current entry
	removable := isRemovableFreqEntry(currentFreqEntry)

	// insert item into a valid entry
	nextFreqElement := currentFreqElement.Next()
	switch {
	case nextFreqElement == nil || nextFreqElement.Value.(*freqEntry[K, V]).freq > nextFreq:
		if removable {
			currentFreqEntry.freq = nextFreq
			nextFreqElement = currentFreqElement
		} else {
			nextFreqElement = c.freqList.InsertAfter(&freqEntry[K, V]{
				freq:  nextFreq,
				items: make(map[*lfuItem[K, V]]struct{}),
			}, currentFreqElement)
		}
	case nextFreqElement.Value.(*freqEntry[K, V]).freq == nextFreq:
		if removable {
			c.freqList.Remove(currentFreqElement)
		}
	default:
		panic("unreachable")
	}
	nextFreqElement.Value.(*freqEntry[K, V]).items[item] = struct{}{}
	item.freqElement = nextFreqElement
}

// evict removes the least frequence item from the cache.
func (c *LFUCache[K, V]) evict(count int) {
	entry := c.freqList.Front()
	for i := 0; i < count; {
		if entry == nil {
			return
		} else {
			for item := range entry.Value.(*freqEntry[K, V]).items {
				if i >= count {
					return
				}
				c.removeItem(item)
				i++
			}
			entry = entry.Next()
		}
	}
}

// Has checks if key exists in cache
func (c *LFUCache[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.has(key, time.Time{})
}

func (c *LFUCache[K, V]) has(key K, now time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now, c.clock)
}

// Remove removes the provided key from the cache.
func (c *LFUCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *LFUCache[K, V]) remove(key K) bool {
	if item, ok := c.items[key]; ok {
		c.removeItem(item)
		return true
	}
	return false
}

// removeElement is used to remove a given list element from the cache
func (c *LFUCache[K, V]) removeItem(item *lfuItem[K, V]) {
	entry := item.freqElement.Value.(*freqEntry[K, V])
	delete(c.items, item.key)
	delete(entry.items, item)
	if isRemovableFreqEntry(entry) {
		c.freqList.Remove(item.freqElement)
	}

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
func (c *LFUCache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *LFUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all key-value pairs from the cache
func (c *LFUCache[K, V]) Clear() {
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

// IsExpired returns boolean value whether this item is expired or not.
func (it *lfuItem[K, V]) IsExpired(now time.Time, clock Clock) bool {
	if it.expiration.IsZero() {
		return false
	}
	if now.IsZero() {
		now = clock.Now()
	}
	return it.expiration.Before(now)
}

func isRemovableFreqEntry[K comparable, V any](entry *freqEntry[K, V]) bool {
	return entry.freq != 0 && len(entry.items) == 0
}
