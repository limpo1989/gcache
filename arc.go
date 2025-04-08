package gcache

import (
	"container/list"
	"context"
	"errors"
	"time"

	"github.com/limpo1989/gcache/internal/pqueue"
)

// Constantly balances between LRU and LFU, to improve the combined result.
type ARC[K comparable, V any] struct {
	baseCache[K, V]
	items map[K]*arcItem[K, V]

	part int
	t1   *arcList[K]
	t2   *arcList[K]
	b1   *arcList[K]
	b2   *arcList[K]
}

func newARC[K comparable, V any](cb *CacheBuilder[K, V]) *ARC[K, V] {
	c := &ARC[K, V]{}
	buildCache(&c.baseCache, cb)
	c.fetch = c.getValue

	c.init()
	return c
}

func (c *ARC[K, V]) init() {
	c.items = make(map[K]*arcItem[K, V])
	c.t1 = newARCList[K]()
	c.t2 = newARCList[K]()
	c.b1 = newARCList[K]()
	c.b2 = newARCList[K]()
}

func (c *ARC[K, V]) replace(key K) {
	if !c.isCacheFull() {
		return
	}
	var old K
	if c.t1.Len() > 0 && ((c.b2.Has(key) && c.t1.Len() == c.part) || (c.t1.Len() > c.part)) {
		old = c.t1.RemoveTail()
		c.b1.PushFront(old)
	} else if c.t2.Len() > 0 {
		old = c.t2.RemoveTail()
		c.b2.PushFront(old)
	} else {
		old = c.t1.RemoveTail()
		c.b1.PushFront(old)
	}
	item, ok := c.items[old]
	if ok {
		delete(c.items, old)
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
}

func (c *ARC[K, V]) Set(key K, value *V) error {
	return c.SetWithExpire(key, value, c.expiration)
}

// SetWithExpire a new key-value pair with an expiration time
func (c *ARC[K, V]) SetWithExpire(key K, value *V, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setWithExpire(key, c.obtain(value), expiration)
}

func (c *ARC[K, V]) setWithExpire(key K, value *V, expiration time.Duration) error {
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
		item = &arcItem[K, V]{
			key:        key,
			value:      value,
			expiration: c.calcExpiration(expiration),
		}
		if !item.expiration.IsZero() {
			item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
			c.expirations.Push(item.elem)
		}

		c.items[key] = item
	}

	defer func() {
		if c.addedFunc != nil {
			c.addedFunc(key, value)
		}
	}()

	if c.t1.Has(key) || c.t2.Has(key) {
		return nil
	}

	if elt := c.b1.Lookup(key); elt != nil {
		c.setPart(min(c.size, c.part+max(c.b2.Len()/c.b1.Len(), 1)))
		c.replace(key)
		c.b1.Remove(key, elt)
		c.t2.PushFront(key)
		return nil
	}

	if elt := c.b2.Lookup(key); elt != nil {
		c.setPart(max(0, c.part-max(c.b1.Len()/c.b2.Len(), 1)))
		c.replace(key)
		c.b2.Remove(key, elt)
		c.t2.PushFront(key)
		return nil
	}

	if c.isCacheFull() && c.t1.Len()+c.b1.Len() == c.size {
		if c.t1.Len() < c.size {
			c.b1.RemoveTail()
			c.replace(key)
		} else {
			pop := c.t1.RemoveTail()
			item, ok := c.items[pop]
			if ok {
				delete(c.items, pop)
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
		}
	} else {
		total := c.t1.Len() + c.b1.Len() + c.t2.Len() + c.b2.Len()
		if total >= c.size {
			if total == (2 * c.size) {
				if c.b2.Len() > 0 {
					c.b2.RemoveTail()
				} else {
					c.b1.RemoveTail()
				}
			}
			c.replace(key)
		}
	}
	c.t1.PushFront(key)
	return nil
}

// Get a value from cache pool using key if it exists. If not exists and it has LoaderFunc, it will generate the value using you have specified LoaderFunc method returns value.
func (c *ARC[K, V]) Get(ctx context.Context, key K) (*V, error) {
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
func (c *ARC[K, V]) GetIfPresent(ctx context.Context, key K) (*V, error) {
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
func (c *ARC[K, V]) GetFn(ctx context.Context, key K, fn func(value *V, err error)) {
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
func (c *ARC[K, V]) GetIfPresentFn(ctx context.Context, key K, fn func(value *V, err error)) {
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
func (c *ARC[K, V]) Touch(ctx context.Context, key K) error {
	_, err := c.getValue(key, false, true, true)
	if nil != err && errors.Is(err, ErrKeyNotFound) {
		_, err = c.getWithLoader(ctx, key)
	}
	return err
}

// Compact clear expired items
func (c *ARC[K, V]) Compact() (n int) {
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

func (c *ARC[K, V]) getValue(key K, onLoad, withLock, renewal bool) (*V, error) {

	if withLock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if elt := c.t1.Lookup(key); elt != nil {
		c.t1.Remove(key, elt)
		item := c.items[key]
		if !item.IsExpired(time.Time{}, c.clock) {
			c.t2.PushFront(key)
			if renewal && c.expiration != 0 {
				item.expiration = c.calcExpiration(c.expiration)
				if item.elem != nil {
					c.expirations.Update(item.elem, item.expiration.UnixNano())
				} else {
					item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
					c.expirations.Push(item.elem)
				}
			}
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return item.value, nil
		} else {
			delete(c.items, key)
			c.b1.PushFront(key)
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
	}
	if elt := c.t2.Lookup(key); elt != nil {
		item := c.items[key]
		if !item.IsExpired(time.Time{}, c.clock) {
			c.t2.MoveToFront(elt)
			if renewal && c.expiration != 0 {
				item.expiration = c.calcExpiration(c.expiration)
				if item.elem != nil {
					c.expirations.Update(item.elem, item.expiration.UnixNano())
				} else {
					item.elem = &pqueue.Elem[K]{Value: key, Priority: item.expiration.UnixNano()}
					c.expirations.Push(item.elem)
				}
			}
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return item.value, nil
		} else {
			delete(c.items, key)
			c.t2.Remove(key, elt)
			c.b2.PushFront(key)
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
	}

	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, ErrKeyNotFound
}

func (c *ARC[K, V]) getWithLoader(ctx context.Context, key K) (*V, error) {
	if c.loaderExpireFunc == nil {
		return nil, ErrKeyNotFound
	}
	return c.load(ctx, key, func(val *V, expiration time.Duration) (*V, error) {
		value := c.obtain(val)
		e := c.setWithExpire(key, value, expiration)
		return value, e
	})
}

// Has checks if key exists in cache
func (c *ARC[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.has(key, time.Time{})
}

func (c *ARC[K, V]) has(key K, now time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now, c.clock)
}

// Remove removes the provided key from the cache.
func (c *ARC[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *ARC[K, V]) remove(key K) bool {
	if elt := c.t1.Lookup(key); elt != nil {
		c.t1.Remove(key, elt)
		item := c.items[key]
		delete(c.items, key)
		c.b1.PushFront(key)
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

	if elt := c.t2.Lookup(key); elt != nil {
		c.t2.Remove(key, elt)
		item := c.items[key]
		delete(c.items, key)
		c.b2.PushFront(key)
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
func (c *ARC[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *ARC[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Clear removes all key-value pairs from the cache
func (c *ARC[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, item := range c.items {
		if item.elem != nil {
			c.expirations.Remove(item.elem)
		}
		if c.purgeVisitorFunc != nil {
			c.purgeVisitorFunc(item.key, item.value)
		}
		if nil != c.allocator {
			c.allocator.Free(item.value)
		}
		item.value = nil
		item.elem = nil
	}

	c.init()
}

func (c *ARC[K, V]) setPart(p int) {
	if c.isCacheFull() {
		c.part = p
	}
}

func (c *ARC[K, V]) isCacheFull() bool {
	return (c.t1.Len() + c.t2.Len()) == c.size
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *arcItem[K, V]) IsExpired(now time.Time, clock Clock) bool {
	if it.expiration.IsZero() {
		return false
	}
	if now.IsZero() {
		now = clock.Now()
	}
	return it.expiration.Before(now)
}

type arcList[K comparable] struct {
	l    *list.List
	keys map[K]*list.Element
}

type arcItem[K comparable, V any] struct {
	key        K
	value      *V
	expiration time.Time
	elem       *pqueue.Elem[K]
}

func newARCList[K comparable]() *arcList[K] {
	return &arcList[K]{
		l:    list.New(),
		keys: make(map[K]*list.Element),
	}
}

func (al *arcList[K]) Has(key K) bool {
	_, ok := al.keys[key]
	return ok
}

func (al *arcList[K]) Lookup(key K) *list.Element {
	elt := al.keys[key]
	return elt
}

func (al *arcList[K]) MoveToFront(elt *list.Element) {
	al.l.MoveToFront(elt)
}

func (al *arcList[K]) PushFront(key K) {
	if elt, ok := al.keys[key]; ok {
		al.l.MoveToFront(elt)
		return
	}
	elt := al.l.PushFront(key)
	al.keys[key] = elt
}

func (al *arcList[K]) Remove(key K, elt *list.Element) {
	delete(al.keys, key)
	al.l.Remove(elt)
}

func (al *arcList[K]) RemoveTail() K {
	elt := al.l.Back()
	al.l.Remove(elt)

	key := elt.Value.(K)
	delete(al.keys, key)

	return key
}

func (al *arcList[K]) Len() int {
	return al.l.Len()
}
