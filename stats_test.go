package gcache

import (
	"context"
	"testing"
)

func TestStats(t *testing.T) {
	var cases = []struct {
		hit  int
		miss int
		rate float64
	}{
		{3, 1, 0.75},
		{0, 1, 0.0},
		{3, 0, 1.0},
		{0, 0, 0.0},
	}

	for _, cs := range cases {
		st := &stats{}
		for i := 0; i < cs.hit; i++ {
			st.IncrHitCount()
		}
		for i := 0; i < cs.miss; i++ {
			st.IncrMissCount()
		}
		if rate := st.HitRate(); rate != cs.rate {
			t.Errorf("%v != %v", rate, cs.rate)
		}
	}
}

func getter(ctx context.Context, key int) (*int, error) {
	var value = key
	return &value, nil
}

func TestCacheStats(t *testing.T) {
	var value int
	var cases = []struct {
		builder func() Cache[int, int]
		rate    float64
	}{
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).Simple().Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).LRU().Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).LFU().Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).ARC().Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).
					Simple().
					LoaderFunc(getter).
					Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).
					LRU().
					LoaderFunc(getter).
					Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).
					LFU().
					LoaderFunc(getter).
					Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
		{
			builder: func() Cache[int, int] {
				cc := New[int, int](32).
					ARC().
					LoaderFunc(getter).
					Build()
				cc.Set(0, &value)
				cc.Get(context.Background(), 0)
				cc.Get(context.Background(), 1)
				return cc
			},
			rate: 0.5,
		},
	}

	for i, cs := range cases {
		cc := cs.builder()
		if rate := cc.HitRate(); rate != cs.rate {
			t.Errorf("case-%v: %v != %v", i, rate, cs.rate)
		}
	}
}
