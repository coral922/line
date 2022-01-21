package line

import (
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

type Queue interface {
	Enqueue(item *M, priority ...uint)
	Dequeue() *M
	Len() int
	Close()
	IsClosed() bool
}

// line settings
type lineOption struct {
	maxQueueLen int
	pqSupported bool
	customQueue Queue
}

type LineOption func(*lineOption)

// WithMaxQueueLen sets max length of the input queue.
// Not effective when using custom queue.
func WithMaxQueueLen(max int) LineOption {
	return func(l *lineOption) {
		l.maxQueueLen = max
	}
}

// WithPQSupported sets priority queue supported.
func WithPQSupported() LineOption {
	return func(l *lineOption) {
		l.pqSupported = true
	}
}

// WithCustomQueue sets custom input queue.
func WithCustomQueue(q Queue) LineOption {
	return func(l *lineOption) {
		l.customQueue = q
	}
}

// New line with options
func New(option ...LineOption) *Line {
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
		l.q = newQueue(l.pqSupported, l.maxQueueLen)
	}
	return l
}

// IsOpen shows whether the line is fetching from the queue.
func (l *Line) IsOpen() bool {
	return l.opened.Val()
}

// AppendStages append stages at specific position.
// if you don't pass the after param or after param not exist in current line,
// stages will be appended to the last by default.
// if after param is empty string, stages will be inserted to the front.
// When line is running, it'll stop the line firstly and rerun after operation been done.
func (l *Line) AppendStages(stages []*Stage, after ...string) *Line {
	if len(stages) == 0 {
		return l
	}
	needRun := l.StopAndWait()
	l.mu.Lock()
	defer l.mu.Unlock()
	last := l.first
	var afterLast *Stage
	for last != nil && last.next != nil {
		last = last.next
	}
	if len(after) > 0 {
		if after[0] == "" {
			last = nil
			afterLast = l.first
		} else if l.stages[after[0]] != nil {
			last = l.stages[after[0]]
			afterLast = last.next
		}
	}
	for _, s := range stages {
		stg := s
		if _, e := l.stages[stg.name]; e {
			panic(ErrDupName)
		}
		if last == nil {
			l.first = stg
			l.first.setInputCh(l.sourceCh)
		} else {
			last.setNext(stg)
		}
		stg.initWorkers()
		l.stages[stg.name] = stg
		last = stg
	}
	last.setNext(afterLast)
	if needRun {
		l.Run()
	}
	return l
}

// SetStages set your stages.
// When line is running, it'll stop the line firstly and rerun after operation been done.
func (l *Line) SetStages(stages []*Stage) *Line {
	needRun := l.StopAndWait()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.removeAllStages()
	if len(stages) == 0 {
		return l
	}
	var last *Stage
	for _, s := range stages {
		stg := s
		if _, e := l.stages[stg.name]; e {
			panic(ErrDupName)
		}
		if l.first == nil {
			l.first = stg
			l.first.pre = nil
			l.first.setInputCh(l.sourceCh)
		} else {
			last.setNext(stg)
		}
		stg.initWorkers()
		l.stages[stg.name] = stg
		last = stg
	}
	if last != nil {
		last.setNext(nil)
	}
	if needRun {
		l.Run()
	}
	return l
}

// RemoveStage remove stage by giving name.
// When line is running, it'll stop the line firstly and rerun after operation been done.
func (l *Line) RemoveStage(stageName string) *Line {
	l.mu.Lock()
	defer l.mu.Unlock()
	toRm := l.stages[stageName]
	if toRm == nil {
		// stage not exist, do nothing.
		return l
	}
	needRun := l.StopAndWait()
	toRm.stop()
	if toRm.getPre() != nil {
		toRm.getPre().setNext(toRm.getNext())
	} else {
		n := toRm.getNext()
		if n == nil {
			l.first = nil
		} else {
			n.setInputCh(l.sourceCh)
			l.first = n
			l.first.pre = nil
		}
	}
	delete(l.stages, toRm.name)
	if needRun {
		l.Run()
	}
	return l
}

// removeAllStages remove all stages.
func (l *Line) removeAllStages() {
	l.mustClosed()
	l.stages = make(map[string]*Stage)
	u := l.first
	l.first = nil
	for u != nil {
		u.stop()
		u = u.getNext()
	}
}

// GetStage returns Stage with giving name.
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
		l.sigInputLoopExited = make(chan struct{})
		go l.asyncInputLoop()
	}
}

// Stop stops fetching item from queue.
func (l *Line) Stop() (switched bool) {
	if l.opened.Cas(true, false) {
		l.q.Enqueue(nil)
		return true
	}
	return false
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

// mustClosed ensures that line is not open.
func (l *Line) mustClosed() {
	if l.IsOpen() {
		panic(ErrAlreadyOpen)
	}
}

// mustHasStage ensures that line has at least one stage.
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

// WithPriority sets priority of the input.
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

// asyncInputLoop continuously fetch item from queue to input channel.
// it'll close sigInputLoopExited channel and return when either line or queue is closed.
func (l *Line) asyncInputLoop() {
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
