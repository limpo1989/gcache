package gcache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestLFUGet(t *testing.T) {
	size := 1000
	numbers := 1000

	gc := buildTestLoadingCache[string, string](t, TypeLfu, size, loader)
	testSetCache(t, gc, numbers)
	testGetCache(t, gc, numbers)
}

func TestLoadingLFUGet(t *testing.T) {
	size := 1000
	numbers := 1000

	gc := buildTestLoadingCache[string, string](t, TypeLfu, size, loader)
	testGetCache(t, gc, numbers)
}

func TestLFULength(t *testing.T) {
	gc := buildTestLoadingCache[string, string](t, TypeLfu, 1000, loader)
	gc.Get(context.Background(), "test1")
	gc.Get(context.Background(), "test2")
	length := gc.Len()
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", length, expectedLength)
	}
}

func TestLFUEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := 11
	gc := buildTestLoadingCache[string, string](t, TypeLfu, cacheSize, loader)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(context.Background(), fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestLFUGetIFPresent(t *testing.T) {
	testGetIFPresent(t, TypeLfu)
}

func TestLFUHas(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration[string, string](t, TypeLfu, 2, 10*time.Millisecond)

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

func TestLFUFreqListOrder(t *testing.T) {
	gc := buildTestCache[int, int](t, TypeLfu, 5)
	for i := 4; i >= 0; i-- {
		val := i
		gc.Set(i, val)
		for j := 0; j <= i; j++ {
			gc.Get(context.Background(), i)
		}
	}
	if l := gc.(*LFUCache[int, int]).freqList.Len(); l != 6 {
		t.Fatalf("%v != 6", l)
	}
	var i uint
	for e := gc.(*LFUCache[int, int]).freqList.Front(); e != nil; e = e.Next() {
		if e.Value.(*freqEntry[int, int]).freq != i {
			t.Fatalf("%v != %v", e.Value.(*freqEntry[int, int]).freq, i)
		}
		i++
	}
	gc.Remove(1)

	if l := gc.(*LFUCache[int, int]).freqList.Len(); l != 5 {
		t.Fatalf("%v != 5", l)
	}
	val := 1
	gc.Set(1, val)
	if l := gc.(*LFUCache[int, int]).freqList.Len(); l != 5 {
		t.Fatalf("%v != 5", l)
	}
	gc.Get(context.Background(), 1)
	if l := gc.(*LFUCache[int, int]).freqList.Len(); l != 5 {
		t.Fatalf("%v != 5", l)
	}
	gc.Get(context.Background(), 1)
	if l := gc.(*LFUCache[int, int]).freqList.Len(); l != 6 {
		t.Fatalf("%v != 6", l)
	}
}

func TestLFUFreqListLength(t *testing.T) {
	k0, v0 := "k0", "v0"
	k1, v1 := "k1", "v1"

	{
		gc := buildTestCache[string, string](t, TypeLfu, 5)
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 1 {
			t.Fatalf("%v != 1", l)
		}
	}
	{
		gc := buildTestCache[string, string](t, TypeLfu, 5)
		gc.Set(k0, v0)
		for i := 0; i < 5; i++ {
			gc.Get(context.Background(), k0)
		}
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}

	{
		gc := buildTestCache[string, string](t, TypeLfu, 5)
		gc.Set(k0, v0)
		gc.Set(k1, v1)
		for i := 0; i < 5; i++ {
			gc.Get(context.Background(), k0)
			gc.Get(context.Background(), k1)
		}
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}

	{
		gc := buildTestCache[string, string](t, TypeLfu, 5)
		gc.Set(k0, v0)
		gc.Set(k1, v1)
		for i := 0; i < 5; i++ {
			gc.Get(context.Background(), k0)
		}
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
		for i := 0; i < 5; i++ {
			gc.Get(context.Background(), k1)
		}
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}

	{
		gc := buildTestCache[string, string](t, TypeLfu, 5)
		gc.Set(k0, v0)
		gc.Get(context.Background(), k0)
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
		gc.Remove(k0)
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 1 {
			t.Fatalf("%v != 1", l)
		}
		gc.Set(k0, v0)
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 1 {
			t.Fatalf("%v != 1", l)
		}
		gc.Get(context.Background(), k0)
		if l := gc.(*LFUCache[string, string]).freqList.Len(); l != 2 {
			t.Fatalf("%v != 2", l)
		}
	}
}
