package line

import (
	"context"
	"testing"
	"time"
)

func w(f WorkFunc) *worker {
	in, out, errCh := make(chan *M), make(chan *M), make(chan ErrorMsg, 1)
	return newWorker("", &in, &out, errCh, &f, &execOption{})
}

func TestWorker_RunNormal(t *testing.T) {
	worker := w(normal)
	go worker.run()
	m := newM(nil)
	*worker.input <- m
	o := <-*worker.output
	if o != m {
		t.FailNow()
	}
}

func TestWorker_RunErr(t *testing.T) {
	worker := w(retErr)
	go worker.run()
	m := newM(nil)
	*worker.input <- m
	o := <-worker.errCh
	if o.Err.Error() != "i am an error" || o.M != m {
		t.FailNow()
	}
}

func TestWorker_RunTimeout(t *testing.T) {
	worker := w(sleep1)
	worker.execOption.timeout = time.Millisecond
	go worker.run()
	m := newM(nil)
	*worker.input <- m
	o := <-worker.errCh
	if o.Err != context.DeadlineExceeded || o.M != m {
		t.FailNow()
	}
}

func TestWorker_Stop(t *testing.T) {
	worker := w(sleep1)
	go worker.run()
	m := newM(nil)
	at := time.Now()
	*worker.input <- m
	go func() {
		for range *worker.output {
		}
	}()
	worker.gracefullyStop()
	if time.Now().Sub(at).Seconds() < 1 {
		t.FailNow()
	}
	if worker.runningFlag.Val() {
		t.FailNow()
	}
	<-worker.execFlag
}

func TestWorker_RunAsync(t *testing.T) {
	worker := w(normal)
	go worker.run()
	m1, m2, m3, m4 := newM(nil), newM(nil), newM(nil), newM(nil)
	go func() {
		*worker.input <- m1
		if m1 != <-*worker.output {
			t.FailNow()
		}
	}()
	go func() {
		*worker.input <- m2
		if m2 != <-*worker.output {
			t.FailNow()
		}
	}()
	go func() {
		*worker.input <- m3
		if m3 != <-*worker.output {
			t.FailNow()
		}
	}()
	go func() {
		*worker.input <- m4
		if m4 != <-*worker.output {
			t.FailNow()
		}
	}()
	time.Sleep(time.Second)
}
