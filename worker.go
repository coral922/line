package line

import (
	"context"
	"sync"
	"time"
)

type worker struct {
	execOption    execOption
	uuid          string
	input, output *chan *M
	errCh         chan ErrorMsg
	function      *WorkFunc
	close         chan struct{}
	runningFlag   *sBool
	execFlag      chan struct{}
	mu            sync.Mutex
}

type execOption struct {
	timeout time.Duration
}

func newWorker(id string, input, output *chan *M, errCh chan ErrorMsg,
	function *WorkFunc, option execOption) *worker {
	w := &worker{
		uuid:        id,
		input:       input,
		output:      output,
		errCh:       errCh,
		function:    function,
		execOption:  option,
		close:       make(chan struct{}),
		execFlag:    make(chan struct{}),
		runningFlag: newSBool(),
		mu:          sync.Mutex{},
	}
	close(w.execFlag)
	return w
}

func (w *worker) run() {
	if w.runningFlag.Cas(false, true) {
		for {
			select {
			case <-w.close:
				w.runningFlag.Set(false)
				return
			case t := <-*w.input:
				if t == nil {
					return
				}
				w.execFlag = make(chan struct{})
				next, err := w.execWithOptions(t)
				if err != nil {
					w.errCh <- ErrorMsg{"", w.uuid, time.Now(), t, err}
					close(w.execFlag)
					continue
				}
				if next != nil && *w.output != nil {
					*w.output <- next
				} else {
					t.done()
				}
				close(w.execFlag)
			}
		}
	}
}

func (w *worker) gracefullyStop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.runningFlag.Val() {
		w.close <- struct{}{}
	}
}

func (w *worker) execWithOptions(m *M) (*M, error) {
	ctx := w.timeoutCtx()
	resCh := make(chan struct {
		m   *M
		err error
	})
	defer close(resCh)
	go func() {
		defer func() {
			recover()
		}()
		//WILL NOT exit function execution by force
		m, err := (*w.function)(ctx, m)
		resCh <- struct {
			m   *M
			err error
		}{m, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resCh:
		return r.m, r.err
	}
}

func (w *worker) timeoutCtx() ExecContext {
	var c ExecContext
	c.WorkerUUID = w.uuid
	c.Context = context.Background()
	if w.execOption.timeout > 0 {
		ctx, _ := context.WithTimeout(c.Context, w.execOption.timeout)
		c.Context = ctx
	}
	return c
}
