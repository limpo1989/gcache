package pqueue

import (
	"testing"
	"time"
)

func TestHeapQueueBasicOperations(t *testing.T) {
	pq := NewPriorityQueue[int](10)

	// Test Push and Len
	if pq.Len() != 0 {
		t.Errorf("Expected length 0, got %d", pq.Len())
	}

	items := []*Elem[int]{
		{Value: 1, Priority: 3},
		{Value: 2, Priority: 1},
		{Value: 3, Priority: 2},
	}

	for _, item := range items {
		pq.Push(item)
	}

	if pq.Len() != 3 {
		t.Errorf("Expected length 3 after push, got %d", pq.Len())
	}

	// Test Peek
	peeked := pq.Peek()
	if peeked.Value != 2 || peeked.Priority != 1 {
		t.Errorf("Peek expected {2 1}, got {%d %d}", peeked.Value, peeked.Priority)
	}

	// Test Pop order
	first := pq.Pop()
	if first.Value != 2 || first.Priority != 1 {
		t.Errorf("First pop expected {2 1}, got {%d %d}", first.Value, first.Priority)
	}

	second := pq.Pop()
	if second.Value != 3 || second.Priority != 2 {
		t.Errorf("Second pop expected {3 2}, got {%d %d}", second.Value, second.Priority)
	}

	third := pq.Pop()
	if third.Value != 1 || third.Priority != 3 {
		t.Errorf("Third pop expected {1 3}, got {%d %d}", third.Value, third.Priority)
	}

	if pq.Len() != 0 {
		t.Errorf("Expected length 0 after pops, got %d", pq.Len())
	}
}

func TestHeapQueueUpdate(t *testing.T) {
	pq := NewPriorityQueue[string](5)

	items := []*Elem[string]{
		{Value: "a", Priority: 10},
		{Value: "b", Priority: 20},
		{Value: "c", Priority: 30},
	}

	for _, item := range items {
		pq.Push(item)
	}

	// Update priority of "b" to be the highest
	item := items[1]
	pq.Update(item, 5)

	// Now "b" should be at the top
	peeked := pq.Peek()
	if peeked.Value != "b" || peeked.Priority != 5 {
		t.Errorf("After update, peek expected {b 5}, got {%s %d}", peeked.Value, peeked.Priority)
	}

	// Pop should return "b" first
	first := pq.Pop()
	if first.Value != "b" {
		t.Errorf("After update, first pop expected b, got %s", first.Value)
	}
}

func TestHeapQueueRemove(t *testing.T) {
	pq := NewPriorityQueue[float64](5)

	items := []*Elem[float64]{
		{Value: 1.1, Priority: 100},
		{Value: 2.2, Priority: 200},
		{Value: 3.3, Priority: 300},
	}

	for _, item := range items {
		pq.Push(item)
	}

	// Remove the middle item
	itemToRemove := items[1]
	pq.Remove(itemToRemove)

	if pq.Len() != 2 {
		t.Errorf("After remove, expected length 2, got %d", pq.Len())
	}

	// Check remaining items
	first := pq.Pop()
	if first.Value != 1.1 {
		t.Errorf("After remove, first pop expected 1.1, got %f", first.Value)
	}

	second := pq.Pop()
	if second.Value != 3.3 {
		t.Errorf("After remove, second pop expected 3.3, got %f", second.Value)
	}
}

func TestHeapQueueEmpty(t *testing.T) {
	pq := NewPriorityQueue[int](0)

	// Test Peek on empty queue
	defer func() {
		if r := recover(); r == nil {
			t.Error("Peek on empty queue should panic")
		}
	}()
	pq.Peek()

	// Test Pop on empty queue
	defer func() {
		if r := recover(); r == nil {
			t.Error("Pop on empty queue should panic")
		}
	}()
	pq.Pop()
}

func TestHeapQueueWithTimePriority(t *testing.T) {
	pq := NewPriorityQueue[string](10)

	now := time.Now().UnixNano()
	items := []*Elem[string]{
		{Value: "later", Priority: now + 1000},
		{Value: "now", Priority: now},
		{Value: "earlier", Priority: now - 1000},
	}

	for _, item := range items {
		pq.Push(item)
	}

	// Items should come out in order: earlier, now, later
	first := pq.Pop()
	if first.Value != "earlier" {
		t.Errorf("Expected 'earlier', got '%s'", first.Value)
	}

	second := pq.Pop()
	if second.Value != "now" {
		t.Errorf("Expected 'now', got '%s'", second.Value)
	}

	third := pq.Pop()
	if third.Value != "later" {
		t.Errorf("Expected 'later', got '%s'", third.Value)
	}
}

func TestHeapQueueInitialCapacity(t *testing.T) {
	capacity := 100
	pq := NewPriorityQueue[int](capacity)

	// Check initial capacity
	if cap(pq.items) != capacity {
		t.Errorf("Expected initial capacity %d, got %d", capacity, cap(pq.items))
	}

	// Push more than initial capacity
	for i := 0; i < capacity*2; i++ {
		pq.Push(&Elem[int]{Value: i, Priority: int64(i)})
	}

	if pq.Len() != capacity*2 {
		t.Errorf("Expected length %d after pushing, got %d", capacity*2, pq.Len())
	}
}
