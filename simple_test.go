package gcache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSimpleGet(t *testing.T) {
	size := 1000
	gc := buildTestCache[string, string](t, TypeSimple, size)
	testSetCache(t, gc, size)
	testGetCache(t, gc, size)
}

func TestLoadingSimpleGet(t *testing.T) {
	size := 1000
	numbers := 1000
	testGetCache(t, buildTestLoadingCache[string, string](t, TypeSimple, size, loader), numbers)
}

func TestSimpleLength(t *testing.T) {
	gc := buildTestLoadingCache[string, string](t, TypeSimple, 1000, loader)
	gc.Get(context.Background(), "test1")
	gc.Get(context.Background(), "test2")
	length := gc.Len()
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", length, expectedLength)
	}
}

func TestSimpleEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := 11
	gc := buildTestLoadingCache[string, string](t, TypeSimple, cacheSize, loader)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestSimpleUnboundedNoEviction(t *testing.T) {
	numbers := 1000
	size_tracker := 0
	gcu := buildTestLoadingCache[string, string](t, TypeSimple, 0, loader)

	for i := 0; i < numbers; i++ {
		current_size := gcu.Len()
		if current_size != size_tracker {
			t.Errorf("Excepted cache size is %v not %v", current_size, size_tracker)
		}

		_, err := gcu.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		size_tracker++
	}
}

func TestSimpleGetIFPresent(t *testing.T) {
	testGetIFPresent(t, TypeSimple)
}

func TestSimpleHas(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration[string, string](t, TypeSimple, 2, 10*time.Millisecond)

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
