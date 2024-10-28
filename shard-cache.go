package gcache

import (
	"context"
	"time"
)

type sharedCache[K comparable, V any] struct {
	*stats
	caches []Cache[K, V]
	hasher KeyHasherFunc[K]
}

func newShardCache[K comparable, V any](stats *stats, caches []Cache[K, V], hasher KeyHasherFunc[K]) Cache[K, V] {
	sc := &sharedCache[K, V]{stats: stats, caches: caches, hasher: hasher}
	if nil == hasher {
		sc.setDefaultHasher()
	}
	return sc
}

func (s *sharedCache[K, V]) getCache(key K) Cache[K, V] {
	hash := s.hasher(key)
	return s.caches[hash%uintptr(len(s.caches))]
}

func (s *sharedCache[K, V]) Set(key K, value V) error {
	return s.getCache(key).Set(key, value)
}

func (s *sharedCache[K, V]) SetWithExpire(key K, value V, expiration time.Duration) error {
	return s.getCache(key).SetWithExpire(key, value, expiration)
}

func (s *sharedCache[K, V]) Get(ctx context.Context, key K) (V, error) {
	return s.getCache(key).Get(ctx, key)
}

func (s *sharedCache[K, V]) get(key K, onLoad bool) (V, error) {
	return s.getCache(key).get(key, onLoad)
}

func (s *sharedCache[K, V]) GetIFPresent(ctx context.Context, key K) (V, error) {
	return s.getCache(key).GetIFPresent(ctx, key)
}

func (s *sharedCache[K, V]) Compact() (n int) {
	for _, cache := range s.caches {
		n += cache.Compact()
	}
	return
}

func (s *sharedCache[K, V]) GetALL(checkExpired bool) (items map[K]V) {
	for _, cache := range s.caches {
		kvs := cache.GetALL(checkExpired)
		if nil == items {
			items = kvs
		} else {
			for k, v := range kvs {
				items[k] = v
			}
		}
	}
	return
}

func (s *sharedCache[K, V]) Remove(key K) bool {
	return s.getCache(key).Remove(key)
}

func (s *sharedCache[K, V]) Purge() {
	for _, cache := range s.caches {
		cache.Purge()
	}
}

func (s *sharedCache[K, V]) Keys(checkExpired bool) (keys []K) {
	for _, cache := range s.caches {
		keys = append(keys, cache.Keys(checkExpired)...)
	}
	return
}

func (s *sharedCache[K, V]) Len(checkExpired bool) (n int) {
	for _, cache := range s.caches {
		n += cache.Len(checkExpired)
	}
	return
}

func (s *sharedCache[K, V]) Has(key K) bool {
	return s.getCache(key).Has(key)
}
