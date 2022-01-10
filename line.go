package line

import (
	"container/list"
	"reflect"
	"sync"
	"sync/atomic"
)

type Line struct {
	stages             map[string]*Stage
	first              *Stage
	sourceCh           chan *M
	opened             *sBool
	sigInputLoopExited chan struct{}
	highestPriority    *uint32
	mu                 sync.Mutex
	q                  Queue
	lineOption
}

type lineOption struct {
	maxQueueLen int
	customQueue Queue
}

type LineOption func(*lineOption)

func WithMaxQueueLen(max int) LineOption {
	return func(l *lineOption) {
		l.maxQueueLen = max
	}
}

func WithCustomQueue(q Queue) LineOption {
	return func(l *lineOption) {
		l.customQueue = q
	}
}

func NewLine(option ...LineOption) *Line {
	l := &Line{
		stages:             make(map[string]*Stage),
		sourceCh:           make(chan *M),
		opened:             newSBool(),
		sigInputLoopExited: make(chan struct{}),
		highestPriority:    new(uint32),
		mu:                 sync.Mutex{},
	}
	close(l.sigInputLoopExited)
	for _, o := range option {
		o(&l.lineOption)
	}
	if l.customQueue != nil {
		l.q = l.customQueue
	} else {
		l.q = newQueue(l.maxQueueLen)
	}
	return l
}

func (l *Line) IsOpen() bool {
	return l.opened.Val()
}

func (l *Line) mustClosed() {
	if l.IsOpen() {
		panic(ErrAlreadyOpen)
	}
}

func (l *Line) mustHasStage() {
	if len(l.stages) == 0 {
		panic(ErrNoStage)
	}
}

// input settings
type inputOption struct {
	priority     uint
	highestPrior bool
	asSlice      bool
	wg           *sync.WaitGroup
}

type InputOption func(*inputOption)

// WithPriority sets priority of the input queue.
func WithPriority(priority uint) InputOption {
	return func(option *inputOption) {
		option.priority = priority
	}
}

// WithPriority sets priority to the highest.
func WithHighestPriority() InputOption {
	return func(option *inputOption) {
		option.highestPrior = true
	}
}

// WithBatch represents treating the input param as a input slice.
// Be sure of your input param's kind is SLICE when using this option.
func WithBatch() InputOption {
	return func(option *inputOption) {
		option.asSlice = true
	}
}

// WithWait option makes the Input function return after all the input objects been done.
func WithWait() InputOption {
	return func(option *inputOption) {
		option.wg = &sync.WaitGroup{}
	}
}

// Input pushes your input object(s) to input queue with giving option.
func (l *Line) Input(obj interface{}, opt ...InputOption) {
	var in inputOption
	for _, o := range opt {
		o(&in)
	}
	priority := in.priority
	if in.highestPrior {
		priority = uint(atomic.AddUint32(l.highestPriority, 1))
	}
	if in.wg != nil {
		c := 1
		if in.asSlice {
			c = reflect.ValueOf(obj).Len()
		}
		in.wg.Add(c)
	}
	if !in.asSlice {
		l.q.Enqueue(newM(obj).setWG(in.wg), priority)
	} else {
		rObj := reflect.ValueOf(obj)
		sliceLength := rObj.Len()
		for i := 0; i < sliceLength; i++ {
			l.q.Enqueue(newM(rObj.Index(i).Interface()).setWG(in.wg), priority)
		}
	}
	if in.wg != nil {
		in.wg.Wait()
	}
}

// InputAndWait pushes your input object(s) to input queue and wait until all been done.
func (l *Line) InputAndWait(obj interface{}, opt ...InputOption) {
	l.Input(obj, append(opt, WithWait())...)
}

func (l *Line) AppendStage() {
	//TODO
}

// SetStages set your stages.
// When line is running, it'll stop the line and rerun after stages been set.
func (l *Line) SetStages(stages ...*Stage) *Line {
	needRun := l.StopAndWait()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.removeAllStages()
	var last *Stage
	for _, s := range stages {
		stg := s
		if _, e := l.stages[stg.name]; e {
			panic(ErrDupName)
		}
		if l.first == nil {
			l.first = stg
			l.first.setInputCh(l.sourceCh)
		} else {
			last.setNext(stg)
		}
		stg.initWorkers()
		l.stages[stg.name] = stg
		last = stg
	}
	if needRun {
		l.Run()
	}
	return l
}

func (l *Line) removeAllStages() {
	l.mustClosed()
	l.stages = make(map[string]*Stage)
	u := l.first
	l.first = nil
	for u != nil {
		u.setInputCh(nil)
		go u.stop()
		u = u.getNext()
	}
}

func (l *Line) GetStage(stageName string) *Stage {
	return l.stages[stageName]
}

// Run starts the line.
// If the line is already started, it does nothing.
func (l *Line) Run() {
	l.mustHasStage()
	u := l.first
	for u != nil {
		u.listen()
		u = u.getNext()
	}
	if l.opened.Cas(false, true) {
		go l.asyncInputLoop()
	}
}

// Stop stops fetching item from queue.
func (l *Line) Stop() (switched bool) {
	return l.opened.Cas(true, false)
}

// StopAndWait stops fetching item from queue and wait until nothing is running.
func (l *Line) StopAndWait() (switched bool) {
	switched = l.Stop()
	<-l.sigInputLoopExited
	s := l.first
	for s != nil {
		s.returnIfIdle()
		s = s.next
	}
	return
}

// asyncInputLoop continuously fetch item from queue to input channel.
// it'll close sigInputLoopExited channel and return when either line or queue is closed.
func (l *Line) asyncInputLoop() {
	l.sigInputLoopExited = make(chan struct{})
	defer close(l.sigInputLoopExited)
	for l.opened.Val() {
		i := l.q.Dequeue()
		if i == nil {
			if l.q.IsClosed() {
				break
			}
			continue
		}
		if !l.opened.Val() {
			l.q.Enqueue(i)
			break
		}
		l.sourceCh <- i
	}
}

func (l *Line) Destroy() {
	fu := l.first
	for fu != nil {
		fu.stop()
		close(fu.outputCh)
		fu = fu.getNext()
	}
	l.stages = nil
	l.first = nil
}

type Queue interface {
	Enqueue(item *M, priority ...uint)
	Dequeue() *M
	Len() int
	Close()
	IsClosed() bool
}

type defaultQueue struct {
	limit  int
	list   *list.List
	closed *sBool
	mu     sync.Mutex
	c      chan *M
}

func newQueue(limit ...int) *defaultQueue {
	q := &defaultQueue{
		closed: newSBool(),
	}
	if len(limit) > 0 && limit[0] > 0 {
		q.limit = limit[0]
		q.c = make(chan *M, limit[0])
	} else {
		q.list = list.New()
		q.mu = sync.Mutex{}
	}
	return q
}

func (q *defaultQueue) Enqueue(i *M, priority ...uint) {
	//TODO: implements priority queue
	if q.limit > 0 {
		q.c <- i
	} else {
		if q.closed.Val() {
			panic("queue already closed")
		}
		q.mu.Lock()
		q.list.PushFront(i)
		q.mu.Unlock()
	}
}

func (q *defaultQueue) Dequeue() *M {
	if q.limit > 0 {
		return <-q.c
	} else {
		//TODO: low performance
		q.mu.Lock()
		defer q.mu.Unlock()
		for q.list.Len() == 0 && !q.closed.Val() {
		}
		i := q.list.Back()
		if i != nil {
			q.list.Remove(i)
			return i.Value.(*M)
		}
		return nil
	}
}

func (q *defaultQueue) Len() int {
	if q.limit > 0 {
		return len(q.c)
	} else {
		return q.list.Len()
	}
}

func (q *defaultQueue) IsClosed() bool {
	return q.closed.Val()
}

func (q *defaultQueue) Close() {
	q.closed.Set(true)
	if q.limit > 0 {
		close(q.c)
	}
}
