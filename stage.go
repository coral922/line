package line

import (
	"fmt"
	"sync"
	"time"
)

type Stage struct {
	option            *stageOption
	name              string
	workFunc          WorkFunc
	inputCh, outputCh chan *M
	errCh             chan ErrorMsg
	workers           []*worker
	next, pre         *Stage
	active            *sBool
	close             chan struct{}
	mu                sync.Mutex
}

type stageOption struct {
	workerNum    int
	errorHandler ErrHandler
	execOption   execOption
}

type StageOption func(o *stageOption)

func WithTimeout(timeout time.Duration) StageOption {
	return func(o *stageOption) {
		o.execOption.timeout = timeout
	}
}

func WithWorkerNum(workerNum int) StageOption {
	if workerNum <= 0 {
		workerNum = 1
	}
	return func(o *stageOption) {
		o.workerNum = workerNum
	}
}

func WithErrHandler(handler ErrHandler) StageOption {
	return func(o *stageOption) {
		o.errorHandler = handler
	}
}

func defaultExecOption() execOption {
	return execOption{}
}

func defaultStageOption() *stageOption {
	return &stageOption{
		workerNum:    1,
		errorHandler: defaultErrorHandler,
		execOption:   defaultExecOption(),
	}
}

func defaultErrorHandler(msg ErrorMsg) {
	fmt.Printf("%s stage [worker_id : %s] error: %s \n", msg.StageName, msg.WorkerID, msg.Err)
}

func NewStage(name string, workFunc WorkFunc, option ...StageOption) *Stage {
	s := &Stage{
		name:     name,
		workFunc: workFunc,
		option:   defaultStageOption(),
		errCh:    make(chan ErrorMsg, 1000),
		close:    make(chan struct{}, 1),
		active:   newSBool(),
		mu:       sync.Mutex{},
	}
	for _, o := range option {
		o(s.option)
	}
	return s
}

func (s *Stage) initWorkers() *Stage {
	for i := 0; i < s.option.workerNum; i++ {
		s.workers = append(s.workers, s.newWorker(i))
	}
	return s
}

func (s *Stage) newWorker(index int) *worker {
	return newWorker(
		s.name+"-"+fmt.Sprintf("%d", index),
		&s.inputCh, &s.outputCh, s.errCh,
		&s.workFunc, s.option.execOption)
}

func (s *Stage) isActive() bool {
	return s.active.Val()
}

func (s *Stage) setInputCh(ch chan *M) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inputCh = ch
}

func (s *Stage) getOutputCh() chan *M {
	return s.outputCh
}

func (s *Stage) getNext() *Stage {
	return s.next
}

func (s *Stage) getPre() *Stage {
	return s.pre
}

func (s *Stage) setNext(next *Stage) {
	s.next = next
	if next != nil {
		if s.outputCh == nil {
			s.outputCh = make(chan *M)
		}
		next.setInputCh(s.getOutputCh())
		next.pre = s
	} else {
		s.outputCh = nil
	}
}

func (s *Stage) ResizeWorkerNum(to int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if to <= 0 {
		to = 1
	}
	n := len(s.workers)
	if to == n {
		return
	}
	if to > n {
		for i := n; i < to; i++ {
			w := s.newWorker(i)
			s.workers = append(s.workers, w)
			if s.isActive() {
				go w.run()
			}
		}
	} else {
		if s.isActive() {
			for _, w := range s.workers[:n-to] {
				w.gracefullyStop()
			}
		}
		s.workers = s.workers[n-to : len(s.workers)]
	}
}

func (s *Stage) listen() {
	if s.active.Cas(false, true) {
		go func() {
			for _, w := range s.workers {
				go w.run()
			}
			for {
				select {
				case err := <-s.errCh:
					err.StageName = s.name
					s.option.errorHandler(err)
					err.M.done()
				case <-s.close:
					return
				}
			}
		}()
	}
}

// returnIfIdle assert that no more input from now on and wait until all workers are idle.
// it may return improperly when the line is open.
func (s *Stage) returnIfIdle() {
	for _, w := range s.workers {
		<-w.execFlag
	}
}

func (s *Stage) stop() {
	if s.active.Val() {
		for _, worker := range s.workers {
			worker.gracefullyStop()
		}
		s.close <- struct{}{}
	}
	s.active.Cas(true, false)
}
