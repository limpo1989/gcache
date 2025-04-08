package gcache

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limpo1989/arena"
)

func TestLoaderFunc(t *testing.T) {
	size := 2
	var testCaches = []*CacheBuilder[int, int64]{
		New[int, int64](size).Simple(),
		New[int, int64](size).LRU(),
		New[int, int64](size).LFU(),
		New[int, int64](size).ARC(),
	}
	for _, builder := range testCaches {
		var testCounter int64
		counter := 1000
		cache := builder.
			LoaderFunc(func(ctx context.Context, key int) (*int64, error) {
				time.Sleep(10 * time.Millisecond)
				value := atomic.AddInt64(&testCounter, 1)
				return &value, nil
			}).
			EvictedFunc(func(key int, value *int64) {
				panic(key)
			}).Build()

		var wg sync.WaitGroup
		for i := 0; i < counter; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cache.Get(context.Background(), 0)
				if err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()

		if testCounter != 1 {
			t.Errorf("testCounter != %v", testCounter)
		}
	}
}

func TestLoaderExpireFuncWithoutExpire(t *testing.T) {
	size := 2
	var testCaches = []*CacheBuilder[int, int64]{
		New[int, int64](size).Simple(),
		New[int, int64](size).LRU(),
		New[int, int64](size).LFU(),
		New[int, int64](size).ARC(),
	}
	for _, builder := range testCaches {
		var testCounter int64
		counter := 1000
		cache := builder.
			LoaderExpireFunc(func(ctx context.Context, key int) (*int64, time.Duration, error) {
				value := atomic.AddInt64(&testCounter, 1)
				return &value, 0, nil
			}).
			EvictedFunc(func(key int, value *int64) {
				panic(key)
			}).Build()

		var wg sync.WaitGroup
		for i := 0; i < counter; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cache.Get(context.Background(), 0)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		wg.Wait()

		if testCounter != 1 {
			t.Errorf("testCounter != %v", testCounter)
		}
	}
}

func TestLoaderExpireFuncWithExpire(t *testing.T) {
	size := 2
	var testCaches = []*CacheBuilder[int, int64]{
		New[int, int64](size).Simple(),
		New[int, int64](size).LRU(),
		New[int, int64](size).LFU(),
		New[int, int64](size).ARC(),
	}
	for _, builder := range testCaches {
		var testCounter int64
		counter := 1000
		expire := 200 * time.Millisecond
		cache := builder.
			LoaderExpireFunc(func(ctx context.Context, key int) (*int64, time.Duration, error) {
				value := atomic.AddInt64(&testCounter, 1)
				return &value, expire, nil
			}).
			Build()

		var wg sync.WaitGroup
		for i := 0; i < counter; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cache.Get(context.Background(), 0)
				if err != nil {
					t.Error(err)
				}
			}()
		}
		time.Sleep(expire) // Waiting for key expiration
		for i := 0; i < counter; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cache.Get(context.Background(), 0)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		wg.Wait()

		if testCounter != 2 {
			t.Errorf("testCounter != %v", testCounter)
		}
	}
}

func TestLoaderPurgeVisitorFunc(t *testing.T) {
	size := 7
	tests := []struct {
		name         string
		cacheBuilder *CacheBuilder[int, int64]
	}{
		{
			name:         "simple",
			cacheBuilder: New[int, int64](size).Simple(),
		},
		{
			name:         "lru",
			cacheBuilder: New[int, int64](size).LRU(),
		},
		{
			name:         "lfu",
			cacheBuilder: New[int, int64](size).LFU(),
		},
		{
			name:         "arc",
			cacheBuilder: New[int, int64](size).ARC(),
		},
	}

	for _, test := range tests {
		var purgeCounter, evictCounter, loaderCounter int64
		counter := 1000
		cache := test.cacheBuilder.
			LoaderFunc(func(ctx context.Context, key int) (*int64, error) {
				value := atomic.AddInt64(&loaderCounter, 1)
				return &value, nil
			}).
			EvictedFunc(func(key int, value *int64) {
				atomic.AddInt64(&evictCounter, 1)
			}).
			PurgeVisitorFunc(func(k int, v *int64) {
				atomic.AddInt64(&purgeCounter, 1)
			}).
			Build()

		var wg sync.WaitGroup
		for i := 0; i < counter; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cache.Get(context.Background(), i)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		wg.Wait()

		if loaderCounter != int64(counter) {
			t.Errorf("%s: loaderCounter != %v", test.name, loaderCounter)
		}

		cache.Clear()

		if evictCounter+purgeCounter != loaderCounter {
			t.Logf("%s: evictCounter: %d", test.name, evictCounter)
			t.Logf("%s: purgeCounter: %d", test.name, purgeCounter)
			t.Logf("%s: loaderCounter: %d", test.name, loaderCounter)
			t.Errorf("%s: load != evict+purge", test.name)
		}
	}
}

func TestDeserializeFunc(t *testing.T) {
	var cases = []struct {
		tp string
	}{
		{TypeSimple},
		{TypeLru},
		{TypeLfu},
		{TypeArc},
	}

	for _, cs := range cases {
		key1, value1 := "key1", "value1"
		key2, value2 := "key2", "value2"
		cc := New[string, string](32).
			EvictType(cs.tp).
			LoaderFunc(func(ctx context.Context, k string) (*string, error) {
				return &value1, nil
			}).
			Build()
		v, err := cc.Get(context.Background(), key1)
		if err != nil {
			t.Fatal(err)
		}
		if *v != value1 {
			t.Errorf("%v != %v", v, value1)
		}
		v, err = cc.Get(context.Background(), key1)
		if err != nil {
			t.Fatal(err)
		}
		if *v != value1 {
			t.Errorf("%v != %v", v, value1)
		}
		if err := cc.Set(key2, &value2); err != nil {
			t.Error(err)
		}
		v, err = cc.Get(context.Background(), key2)
		if err != nil {
			t.Error(err)
		}
		if *v != value2 {
			t.Errorf("%v != %v", v, value2)
		}
	}
}

func TestExpiredItems(t *testing.T) {
	var tps = []string{
		TypeSimple,
		TypeLru,
		TypeLfu,
		TypeArc,
	}
	for _, tp := range tps {
		t.Run(tp, func(t *testing.T) {
			testExpiredItems(t, tp)
		})
	}
}

func TestTouchItems(t *testing.T) {
	var tps = []string{
		TypeSimple,
		TypeLru,
		TypeLfu,
		TypeArc,
	}
	for _, tp := range tps {
		t.Run(tp, func(t *testing.T) {
			testTouchItems(t, tp)
		})
	}
}

type testPlayer struct {
	id         int
	name       string
	level      int
	createTime time.Time
}

const largeSize = 1000 * 1000 * 10

func TestHeapCache(t *testing.T) {

	var now = time.Now()

	cache := New[int, testPlayer](largeSize).
		LoaderFunc(func(ctx context.Context, idx int) (*testPlayer, error) {
			return &testPlayer{id: idx, name: "test_player", level: idx, createTime: now.Add(time.Second * time.Duration(idx))}, nil
		}).
		Simple().
		Build()

	for i := 0; i < largeSize; i++ {
		p, err := cache.Get(context.Background(), i)
		if nil != err {
			t.Fatal(err)
		}

		if p.id != i {
			t.Errorf("%d != %d", i, p.id)
		}
	}

	start := time.Now()
	runtime.GC()
	var memStat runtime.MemStats
	runtime.ReadMemStats(&memStat)
	t.Logf("Heap Cache GC took time: %v, cache size: %d, living objects: %d", time.Since(start), cache.Len(), memStat.HeapObjects)
	runtime.KeepAlive(cache)
}

func TestArenaCache(t *testing.T) {

	var now = time.Now()

	// 或者直接初始化local location
	_ = time.Local.String()

	cache := New[int, testPlayer](largeSize).
		LoaderFunc(func(ctx context.Context, idx int) (*testPlayer, error) {
			return &testPlayer{id: idx, name: "test_player", level: idx, createTime: now.Add(time.Second * time.Duration(idx))}, nil
		}).
		Arena(arena.WithChunkSize(1024 * 1024)).
		Simple().
		Build()

	for i := 0; i < largeSize; i++ {
		cache.GetFn(context.Background(), i, func(p *testPlayer, err error) {
			if nil != err {
				t.Fatal(err)
			}

			if p.id != i {
				t.Errorf("%d != %d", i, p.id)
			}
		})
	}

	start := time.Now()
	runtime.GC()
	var memStat runtime.MemStats
	runtime.ReadMemStats(&memStat)
	t.Logf("Arena Cache GC took time: %v, cache size: %d, living objects: %d", time.Since(start), cache.Len(), memStat.HeapObjects)
	runtime.KeepAlive(cache)
}

func TestSlowLoaderFunc(t *testing.T) {
	size := 2000
	var testCaches = []*CacheBuilder[int, int64]{
		New[int, int64](size).Simple(),
		New[int, int64](size).LRU(),
		New[int, int64](size).LFU(),
		New[int, int64](size).ARC(),
	}
	for _, builder := range testCaches {
		var startTime = time.Now()
		counter := 1000
		cache := builder.
			LoaderFunc(func(ctx context.Context, key int) (*int64, error) {
				time.Sleep(10 * time.Millisecond)
				value := int64(key)
				return &value, nil
			}).Build()

		var wg sync.WaitGroup
		for i := 0; i < counter; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cache.Get(context.Background(), i)
				if err != nil {
					t.Error(err)
				}
			}()
		}

		wg.Wait()
		t.Logf("load all items cost %v", time.Since(startTime))
	}
}
