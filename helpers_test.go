package gcache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func loader[K comparable, V any](ctx context.Context, key K) (V, error) {

	var zero V
	var value = reflect.New(reflect.TypeOf(zero)).Elem()
	switch value.Kind() {
	case reflect.String:
		value.SetString(fmt.Sprintf("valueFor%v", key))
	default:
	}

	return value.Interface().(V), nil
}

func testSetCache(t *testing.T, gc Cache[string, string], numbers int) {
	for i := 0; i < numbers; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value, err := loader[string, string](context.Background(), key)
		if err != nil {
			t.Error(err)
			return
		}
		gc.Set(key, value)
	}
}

func testGetCache(t *testing.T, gc Cache[string, string], numbers int) {
	for i := 0; i < numbers; i++ {
		key := fmt.Sprintf("Key-%d", i)
		v, err := gc.Get(context.Background(), key)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		expectedV, _ := loader[string, string](context.Background(), key)
		if *v != expectedV {
			t.Errorf("Expected value is %v, not %v", expectedV, *v)
		}
	}
}

func testGetIFPresent(t *testing.T, evT string) {
	var value = "value"
	cache :=
		New[any, string](8).
			EvictType(evT).
			LoaderFunc(
				func(ctx context.Context, key any) (string, error) {
					return value, nil
				}).
			Build()

	_, err := cache.GetIfPresent(context.Background(), "key")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err should not be %v", err)
	}
}

func setItemsByRange(t *testing.T, c Cache[int, int], start, end int) {
	for i := start; i < end; i++ {
		val := i
		if err := c.Set(i, val); err != nil {
			t.Error(err)
		}
	}
}

func touchItemsByRange(t *testing.T, c Cache[int, int], start, end int) {
	for i := start; i < end; i++ {
		if err := c.Touch(t.Context(), i); err != nil {
			t.Error(err)
		}
	}
}

func keysToMap(keys []any) map[any]struct{} {
	m := make(map[any]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

func checkItemsByRange(t *testing.T, keys []any, m map[any]any, size, start, end int) {
	if len(keys) != size {
		t.Fatalf("%v != %v", len(keys), size)
	} else if len(m) != size {
		t.Fatalf("%v != %v", len(m), size)
	}
	km := keysToMap(keys)
	for i := start; i < end; i++ {
		if _, ok := km[i]; !ok {
			t.Errorf("keys should contain %v", i)
		}
		v, ok := m[i]
		if !ok {
			t.Errorf("m should contain %v", i)
			continue
		}
		if v.(int) != i {
			t.Errorf("%v != %v", v, i)
			continue
		}
	}
}

func testExpiredItems(t *testing.T, evT string) {
	size := 8
	cache :=
		New[int, int](size).
			Expiration(time.Millisecond).
			EvictType(evT).
			Build()

	setItemsByRange(t, cache, 0, size)

	time.Sleep(time.Millisecond)
	cache.Compact()

	if l := cache.Len(); l != 0 {
		t.Fatalf("GetALL should returns no items, but got length %v", l)
	}

	value := 1
	cache.Set(1, value)
}

func testTouchItems(t *testing.T, evT string) {
	size := 8
	cache :=
		New[int, int](size).
			Expiration(time.Millisecond * 2).
			EvictType(evT).
			Build()

	setItemsByRange(t, cache, 0, size)
	time.Sleep(time.Millisecond)
	touchItemsByRange(t, cache, 0, size)
	time.Sleep(time.Millisecond)
	cache.Compact()

	if l := cache.Len(); l != size {
		t.Fatalf("GetALL should returns %d items, but got length %v", size, l)
	}

	value := 1
	cache.Set(1, value)
}

func getSimpleEvictedFunc[K comparable, V any](t *testing.T) func(K, *V) {
	return func(key K, value *V) {
		t.Logf("Key=%v Value=%v will be evicted.\n", key, *value)
	}
}

func buildTestCache[K comparable, V any](t *testing.T, tp string, size int) Cache[K, V] {
	return New[K, V](size).
		EvictType(tp).
		EvictedFunc(getSimpleEvictedFunc[K, V](t)).
		Build()
}

func buildTestLoadingCache[K comparable, V any](t *testing.T, tp string, size int, loader LoaderFunc[K, V]) Cache[K, V] {
	return New[K, V](size).
		EvictType(tp).
		LoaderFunc(loader).
		EvictedFunc(getSimpleEvictedFunc[K, V](t)).
		Build()
}

func buildTestLoadingCacheWithExpiration[K comparable, V any](t *testing.T, tp string, size int, ep time.Duration) Cache[K, V] {
	return New[K, V](size).
		EvictType(tp).
		Expiration(ep).
		LoaderFunc(loader[K, V]).
		EvictedFunc(getSimpleEvictedFunc[K, V](t)).
		Build()
}
