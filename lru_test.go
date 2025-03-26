package gcache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestLRUGet(t *testing.T) {
	size := 1000
	gc := buildTestCache[string, string](t, TypeLru, size)
	testSetCache(t, gc, size)
	testGetCache(t, gc, size)
}

func TestLoadingLRUGet(t *testing.T) {
	size := 1000
	gc := buildTestLoadingCache[string, string](t, TypeLru, size, loader)
	testGetCache(t, gc, size)
}

func TestLRULength(t *testing.T) {
	gc := buildTestLoadingCache[string, string](t, TypeLru, 1000, loader)
	gc.Get(context.Background(), "test1")
	gc.Get(context.Background(), "test2")
	length := gc.Len()
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", length, expectedLength)
	}
}

func TestLRUEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := 11
	gc := buildTestLoadingCache[string, string](t, TypeLru, cacheSize, loader)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestLRUGetIFPresent(t *testing.T) {
	testGetIFPresent(t, TypeLru)
}

func TestLRUHas(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration[string, string](t, TypeLru, 2, 10*time.Millisecond)

	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			gc.Get(context.Background(), "test1")
			gc.Get(context.Background(), "test2")

			if gc.Has("test0") {
				t.Fatal("should not have test0")
			}
			if !gc.Has("test1") {
				t.Fatal("should have test1")
			}
			if !gc.Has("test2") {
				t.Fatal("should have test2")
			}

			time.Sleep(20 * time.Millisecond)

			if gc.Has("test0") {
				t.Fatal("should not have test0")
			}
			if gc.Has("test1") {
				t.Fatal("should not have test1")
			}
			if gc.Has("test2") {
				t.Fatal("should not have test2")
			}
		})
	}
}
