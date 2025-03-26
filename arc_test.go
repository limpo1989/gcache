package gcache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestARCGet(t *testing.T) {
	size := 1000
	gc := buildTestCache[string, string](t, TypeArc, size)
	testSetCache(t, gc, size)
	testGetCache(t, gc, size)
}

func TestLoadingARCGet(t *testing.T) {
	size := 1000
	numbers := 1000
	testGetCache(t, buildTestLoadingCache[string, string](t, TypeArc, size, loader), numbers)
}

func TestARCLength(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration[string, string](t, TypeArc, 2, time.Millisecond)
	gc.Get(context.Background(), "test1")
	gc.Get(context.Background(), "test2")
	gc.Get(context.Background(), "test3")
	length := gc.Len()
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", expectedLength, length)
	}
	time.Sleep(time.Millisecond)
	gc.Get(context.Background(), "test4")
	gc.Compact()
	length = gc.Len()
	expectedLength = 1
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", expectedLength, length)
	}
}

func TestARCEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := cacheSize + 1
	gc := buildTestLoadingCache[string, string](t, TypeArc, cacheSize, loader)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestARCPurgeCache(t *testing.T) {
	cacheSize := 10
	purgeCount := 0
	gc := New[string, string](cacheSize).
		ARC().
		LoaderFunc(loader[string, string]).
		PurgeVisitorFunc(func(k string, v *string) {
			purgeCount++
		}).
		Build()

	for i := 0; i < cacheSize; i++ {
		_, err := gc.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	gc.Clear()

	if purgeCount != cacheSize {
		t.Errorf("failed to purge everything")
	}
}

func TestARCGetIFPresent(t *testing.T) {
	testGetIFPresent(t, TypeArc)
}

func TestARCHas(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration[string, string](t, TypeArc, 2, 10*time.Millisecond)

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
