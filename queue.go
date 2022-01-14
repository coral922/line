package line

import (
	"container/heap"
	"container/list"
	"sync"
)

func newQueue(pqSupported bool, limit ...int) Queue {
	if pqSupported {
		return newPriorityQueue(limit...)
	}
	return newDefaultQueue(limit...)
}

const (
	defaultQueueSize = 10000 // Size for queue buffer if queue length is not limited.
	defaultBatchSize = 10    // Max per-fetching batch size.
)

type defaultQueue struct {
	limit  int
	c      chan *M
	list   *list.List
	mu     sync.RWMutex
	closed *sBool
	//data writing trigger
	trigger chan struct{}
}

func newDefaultQueue(limit ...int) *defaultQueue {
	q := &defaultQueue{
		closed: newSBool(),
	}
	if len(limit) > 0 && limit[0] > 0 {
		q.limit = limit[0]
		q.c = make(chan *M, limit[0])
	} else {
		q.list = list.New()
		q.mu = sync.RWMutex{}
		q.trigger = make(chan struct{}, 1<<31-1)
		q.c = make(chan *M, defaultQueueSize)
		go q.asyncLoopFromListToChannel()
	}
	return q
}

// asyncLoopFromListToChannel starts an asynchronous goroutine,
// which handles the data synchronization from list <q.list> to channel <q.C>.
func (q *defaultQueue) asyncLoopFromListToChannel() {
	defer func() {
		if q.closed.Val() {
			recover()
		}
	}()
	for !q.closed.Val() {
		<-q.trigger
		for !q.closed.Val() {
			if length := q.listLen(); length > 0 {
				if length > defaultBatchSize {
					length = defaultBatchSize
				}
				for _, v := range q.popList(length) {
					q.c <- v.(*M)
				}
			} else {
				break
			}
		}
		for i := 0; i < len(q.trigger)-1; i++ {
			<-q.trigger
		}
	}
	close(q.c)
}

func (q *defaultQueue) Enqueue(i *M, priority ...uint) {
	if q.limit > 0 {
		q.c <- i
	} else {
		if q.closed.Val() {
			panic("queue already closed")
		}
		q.mu.Lock()
		q.list.PushBack(i)
		q.mu.Unlock()
		if len(q.trigger) < defaultQueueSize {
			q.trigger <- struct{}{}
		}
	}
}

func (q *defaultQueue) Dequeue() *M {
	return <-q.c
}

func (q *defaultQueue) listLen() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.list == nil {
		return 0
	}
	return q.list.Len()
}

func (q *defaultQueue) popList(max int) []interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.list == nil {
		q.list = list.New()
		return []interface{}{}
	}
	length := q.list.Len()
	if length > 0 {
		if max > 0 && max < length {
			length = max
		}
		items := make([]interface{}, length)
		for i := 0; i < length; i++ {
			items[i] = q.list.Remove(q.list.Front())
		}
		return items
	}
	return []interface{}{}
}

func (q *defaultQueue) Len() int {
	var l int
	if q.list != nil {
		l += q.list.Len()
	}
	l += len(q.c)
	return l
}

func (q *defaultQueue) IsClosed() bool {
	return q.closed.Val()
}

func (q *defaultQueue) Close() {
	q.closed.Set(true)
	if q.trigger != nil {
		close(q.trigger)
	}
	if q.limit > 0 {
		close(q.c)
	}
	for range q.c {
	}
}

type priorityQueue struct {
	data    []*item
	limit   chan struct{}
	mu      sync.Mutex
	nextUid uint64
	closed  *sBool
	//data writing trigger
	trigger chan struct{}
}

// An item is something we manage in a priority queue.
type item struct {
	value    *M     // The value of the item;
	priority int    // The priority of the item in the queue.
	uid      uint64 // The unique id of the item in the queue, used for making sort stable.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

func newPriorityQueue(limit ...int) *priorityQueue {
	q := &priorityQueue{
		closed: newSBool(),
	}
	if len(limit) > 0 && limit[0] > 0 {
		q.limit = make(chan struct{}, limit[0])
	}
	q.data = make([]*item, 0)
	q.mu = sync.Mutex{}
	q.trigger = make(chan struct{}, 1<<31-1)
	return q
}

func (p *priorityQueue) Less(i, j int) bool {
	if p.data[i].priority == p.data[j].priority {
		return p.data[i].uid < p.data[j].uid
	}
	return p.data[i].priority > p.data[j].priority
}

func (p *priorityQueue) Swap(i, j int) {
	p.data[i], p.data[j] = p.data[j], p.data[i]
	p.data[i].index = i
	p.data[j].index = j
}

func (p *priorityQueue) Push(x interface{}) {
	n := len(p.data)
	item := x.(*item)
	item.index = n
	p.data = append(p.data, item)
}

func (p *priorityQueue) Pop() interface{} {
	old := p.data
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	p.data = old[0 : n-1]
	return item
}

func (p *priorityQueue) Enqueue(m *M, priority ...uint) {
	if p.closed.Val() {
		panic("queue already closed")
	}
	if p.limit != nil {
		p.limit <- struct{}{}
	}
	if len(priority) == 0 {
		priority = []uint{0}
	}
	it := &item{
		value:    m,
		priority: int(priority[0]),
	}
	p.mu.Lock()
	it.uid = p.nextUid
	p.nextUid++
	heap.Push(p, it)
	p.mu.Unlock()
	p.trigger <- struct{}{}
	return
}

func (p *priorityQueue) Dequeue() *M {
	<-p.trigger
	p.mu.Lock()
	if p.Len() == 0 {
		p.mu.Unlock()
		return nil
	}
	i := heap.Pop(p).(*item)
	p.mu.Unlock()
	if p.limit != nil {
		<-p.limit
	}
	return i.value
}

func (p *priorityQueue) Len() int {
	return len(p.data)
}

func (p *priorityQueue) Close() {
	p.closed.Set(true)
	close(p.trigger)
	if p.limit != nil {
		close(p.limit)
	}
	p.data = make([]*item, 0)
}

func (p *priorityQueue) IsClosed() bool {
	return p.closed.Val()
}
