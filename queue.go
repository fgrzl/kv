package kv

// Queue is an optimized FIFO queue with preallocation and efficient memory management.
type Queue[T any] struct {
	items []T
	head  int
	tail  int // Added tail to track end of queue
}

// NewQueue initializes a queue with a preallocated capacity.
func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{
		items: make([]T, 0, capacity),
		head:  0,
		tail:  0,
	}
}

// Enqueue adds an item efficiently to the end of the queue.
func (q *Queue[T]) Enqueue(item T) {
	if q.tail == len(q.items) && q.head > 0 {
		// Shift items left if head has moved and tail reached end
		copy(q.items, q.items[q.head:q.tail])
		q.tail -= q.head
		q.head = 0
	}
	q.items = append(q.items[:q.tail], item)
	q.tail++
}

// Dequeue removes and returns the first item in the queue.
func (q *Queue[T]) Dequeue() (T, bool) {
	var zero T
	if q.IsEmpty() {
		return zero, false
	}
	item := q.items[q.head]
	q.items[q.head] = zero // Clear reference for GC
	q.head++

	// Shrink if we've dequeued half the items and have significant capacity
	if q.head > q.tail/2 && cap(q.items) > 32 {
		newItems := make([]T, q.tail-q.head, (q.tail-q.head)*2)
		copy(newItems, q.items[q.head:q.tail])
		q.items = newItems
		q.tail -= q.head
		q.head = 0
	}

	return item, true
}

// IsEmpty checks if the queue has no items left.
func (q *Queue[T]) IsEmpty() bool {
	return q.head >= q.tail
}

// Length returns the number of remaining items.
func (q *Queue[T]) Length() int {
	return q.tail - q.head
}

// Reset clears the queue while preserving allocated capacity.
func (q *Queue[T]) Reset() {
	for i := q.head; i < q.tail; i++ {
		var zero T
		q.items[i] = zero // Clear references for GC
	}
	q.head = 0
	q.tail = 0
}
