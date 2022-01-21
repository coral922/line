package line

import (
	"testing"
	"time"
)

type obj struct {
	id string
}

func o(id string) *M {
	return newM(&obj{id: id})
}

func eq(objs ...*M) bool {
	for i := 0; i < len(objs)-1; i += 2 {
		if objs[i] != objs[i+1] {
			return false
		}
	}
	return true
}

func TestPriorityQueue_Len(t *testing.T) {
	o1, o2, o3, o4 := o("1"), o("2"), o("3"), o("4")
	q := newPriorityQueue()
	q.Enqueue(o1)
	q.Enqueue(o2)
	q.Enqueue(o3)
	q.Enqueue(o4)
	if q.Len() != 4 {
		t.FailNow()
	}
}

func TestDefaultQueue_Len(t *testing.T) {
	o1, o2, o3, o4 := o("1"), o("2"), o("3"), o("4")
	q := newDefaultQueue()
	q.Enqueue(o1)
	q.Enqueue(o2)
	q.Enqueue(o3)
	q.Enqueue(o4)
	if q.Len() != 4 {
		t.FailNow()
	}
}

func TestPriorityQueue_Basic(t *testing.T) {
	o1, o2, o3, o4 := o("1"), o("2"), o("3"), o("4")
	q := newPriorityQueue()
	q.Enqueue(o1)
	q.Enqueue(o2)
	q.Enqueue(o3)
	q.Enqueue(o4)
	d1 := q.Dequeue()
	d2 := q.Dequeue()
	d3 := q.Dequeue()
	d4 := q.Dequeue()
	if !eq(o1, d1, o2, d2, o3, d3, o4, d4) {
		t.FailNow()
	}
}

func TestDefaultQueue_Basic(t *testing.T) {
	o1, o2, o3, o4 := o("1"), o("2"), o("3"), o("4")
	q := newDefaultQueue()
	q.Enqueue(o1)
	q.Enqueue(o2)
	q.Enqueue(o3)
	q.Enqueue(o4)
	d1 := q.Dequeue()
	d2 := q.Dequeue()
	d3 := q.Dequeue()
	d4 := q.Dequeue()
	if !eq(o1, d1, o2, d2, o3, d3, o4, d4) {
		t.FailNow()
	}
}

func TestPriorityQueue_WithPrior(t *testing.T) {
	o1, o2, o3, o4, o5 := o("1"), o("2"), o("3"), o("4"), o("5")
	q := newPriorityQueue()
	q.Enqueue(o1, 1)
	q.Enqueue(o2, 2)
	q.Enqueue(o3, 2)
	q.Enqueue(o4, 3)
	q.Enqueue(o5, 4)
	d1 := q.Dequeue()
	d2 := q.Dequeue()
	d3 := q.Dequeue()
	d4 := q.Dequeue()
	d5 := q.Dequeue()
	if !eq(d1, o5, d2, o4, d3, o2, d4, o3, d5, o1) {
		t.FailNow()
	}
}

func TestPriorityQueue_Async(t *testing.T) {
	o1, o2, o3, o4, o5 := o("1"), o("2"), o("3"), o("4"), o("5")
	q := newPriorityQueue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Enqueue(o1, 1)
	go q.Enqueue(o2, 2)
	go q.Enqueue(o3, 2)
	go q.Enqueue(o4, 3)
	go q.Enqueue(o5, 4)
	time.Sleep(10 * time.Millisecond)
	if q.Len() != 0 {
		t.FailNow()
	}
}

func TestDefaultQueue_Async(t *testing.T) {
	o1, o2, o3, o4, o5 := o("1"), o("2"), o("3"), o("4"), o("5")
	q := newDefaultQueue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Dequeue()
	go q.Enqueue(o1, 1)
	go q.Enqueue(o2, 2)
	go q.Enqueue(o3, 2)
	go q.Enqueue(o4, 3)
	go q.Enqueue(o5, 4)
	time.Sleep(10 * time.Millisecond)
	if q.Len() != 0 {
		t.FailNow()
	}
}

func TestDefaultQueue_WithLimit(t *testing.T) {
	o1, o2, o3 := o("1"), o("2"), o("3")
	q := newDefaultQueue(2)
	q.Enqueue(o1, 1)
	q.Enqueue(o2, 1)
	var o3done bool
	go func() {
		q.Enqueue(o3, 1)
		o3done = true
	}()
	time.Sleep(500 * time.Millisecond)
	if o3done {
		t.FailNow()
	}
	q.Dequeue()
	time.Sleep(500 * time.Millisecond)
	if !o3done {
		t.FailNow()
	}
}

func TestPriorityQueue_WithLimit(t *testing.T) {
	o1, o2, o3 := o("1"), o("2"), o("3")
	q := newPriorityQueue(2)
	q.Enqueue(o1, 1)
	q.Enqueue(o2, 1)
	var o3done bool
	go func() {
		q.Enqueue(o3, 1)
		o3done = true
	}()
	time.Sleep(500 * time.Millisecond)
	if o3done {
		t.FailNow()
	}
	q.Dequeue()
	time.Sleep(500 * time.Millisecond)
	if !o3done {
		t.FailNow()
	}
}

func TestPriorityQueue_Close(t *testing.T) {
	o1, o2 := o("1"), o("2")
	q := newPriorityQueue()
	q.Enqueue(o1, 1)
	q.Enqueue(o2, 2)
	q.Close()
	if !q.IsClosed() {
		t.FailNow()
	}
	t.Run("send on closed queue", func(t *testing.T) {
		defer func() {
			recover()
		}()
		q.Enqueue(o1)
		t.FailNow()
	})
	t.Run("receive on closed queue", func(t *testing.T) {
		if !eq(q.Dequeue(), nil) {
			t.FailNow()
		}
	})
}

func TestDefaultQueue_Close(t *testing.T) {
	o1, o2 := o("1"), o("2")
	q := newDefaultQueue()
	q.Enqueue(o1, 1)
	q.Enqueue(o2, 2)
	q.Close()
	if !q.IsClosed() {
		t.FailNow()
	}
	t.Run("send on closed queue", func(t *testing.T) {
		defer func() {
			recover()
		}()
		q.Enqueue(o1)
		t.FailNow()
	})
	t.Run("receive on closed queue", func(t *testing.T) {
		if !eq(q.Dequeue(), nil) {
			t.FailNow()
		}
	})
}
