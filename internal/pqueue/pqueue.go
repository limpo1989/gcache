package pqueue

import "container/heap"

// Elem is something we manage in a priority queue.
type Elem[T any] struct {
	Value    T     // The value of the item; arbitrary.
	Priority int64 // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// Elems implements heap.Interface and holds Items.
type Elems[T any] []*Elem[T]

func (pq Elems[T]) Len() int { return len(pq) }

func (pq Elems[T]) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq Elems[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *Elems[T]) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Elem[T])
	item.index = n
	*pq = append(*pq, item)
}

func (pq *Elems[T]) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

type PriorityQueue[T any] struct {
	items Elems[T]
}

func (pq *PriorityQueue[T]) Push(el *Elem[T]) {
	if nil == el {
		panic("elem is nil")
	}
	heap.Push(&pq.items, el)
}

func (pq *PriorityQueue[T]) Pop() *Elem[T] {
	if len(pq.items) == 0 {
		panic("queue is nil")
	}
	el := heap.Pop(&pq.items)
	return el.(*Elem[T])
}

func (pq *PriorityQueue[T]) Peek() *Elem[T] {
	if len(pq.items) == 0 {
		panic("queue is nil")
	}
	return pq.items[0]
}

// Update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue[T]) Update(el *Elem[T], priority int64) {
	if nil == el {
		panic("elem is nil")
	}
	heap.Remove(&pq.items, el.index)
	el.Priority = priority
	heap.Push(&pq.items, el)
}

func (pq *PriorityQueue[T]) Remove(el *Elem[T]) {
	if nil == el {
		panic("elem is nil")
	}
	heap.Remove(&pq.items, el.index)
}

func (pq *PriorityQueue[T]) Len() int {
	return pq.items.Len()
}

func (pq *PriorityQueue[T]) IsEmpty() bool {
	return pq.Len() == 0
}

func NewPriorityQueue[T any](capacity int) *PriorityQueue[T] {
	return &PriorityQueue[T]{items: make(Elems[T], 0, capacity)}
}
