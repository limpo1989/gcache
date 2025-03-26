package gcache

import (
	"context"
	"hash/maphash"
	"time"
)

type sharedCache[K comparable, V any] struct {
	*stats
	caches []Cache[K, V]
	seed   maphash.Seed
}

func newShardCache[K comparable, V any](stats *stats, caches []Cache[K, V]) Cache[K, V] {
	sc := &sharedCache[K, V]{stats: stats, caches: caches, seed: maphash.MakeSeed()}
	return sc
}

func (s *sharedCache[K, V]) getCache(key K) Cache[K, V] {
	hash := maphash.Comparable(s.seed, key)
	return s.caches[hash%uint64(len(s.caches))]
}

func (s *sharedCache[K, V]) Set(key K, value V) error {
	return s.getCache(key).Set(key, value)
}

func (s *sharedCache[K, V]) SetWithExpire(key K, value V, expiration time.Duration) error {
	return s.getCache(key).SetWithExpire(key, value, expiration)
}

func (s *sharedCache[K, V]) Get(ctx context.Context, key K) (*V, error) {
	return s.getCache(key).Get(ctx, key)
}

func (s *sharedCache[K, V]) GetIfPresent(ctx context.Context, key K) (*V, error) {
	return s.getCache(key).GetIfPresent(ctx, key)
}

func (s *sharedCache[K, V]) GetFn(ctx context.Context, key K, fn func(value *V, err error)) {
	s.getCache(key).GetFn(ctx, key, fn)
}

func (s *sharedCache[K, V]) GetIfPresentFn(ctx context.Context, key K, fn func(value *V, err error)) {
	s.getCache(key).GetIfPresentFn(ctx, key, fn)
}

func (s *sharedCache[K, V]) Touch(ctx context.Context, key K) error {
	return s.getCache(key).Touch(ctx, key)
}

func (s *sharedCache[K, V]) Compact() (n int) {
	for _, cache := range s.caches {
		n += cache.Compact()
	}
	return
}

func (s *sharedCache[K, V]) Remove(key K) bool {
	return s.getCache(key).Remove(key)
}

func (s *sharedCache[K, V]) Clear() {
	for _, cache := range s.caches {
		cache.Clear()
	}
}

func (s *sharedCache[K, V]) Keys() (keys []K) {
	for _, cache := range s.caches {
		keys = append(keys, cache.Keys()...)
	}
	return
}

func (s *sharedCache[K, V]) Len() (n int) {
	for _, cache := range s.caches {
		n += cache.Len()
	}
	return
}

func (s *sharedCache[K, V]) Has(key K) bool {
	return s.getCache(key).Has(key)
}
