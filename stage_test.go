package line

import (
	"context"
	"errors"
	"testing"
	"time"
)

var normal = func(ctx ExecContext, input *M) (output *M, err error) {
	return input, nil
}

var sleep1 = func(ctx ExecContext, input *M) (output *M, err error) {
	time.Sleep(time.Second)
	return input, nil
}

var retErr = func(ctx ExecContext, input *M) (output *M, err error) {
	return nil, errors.New("i am an error")
}

var flag error

var errHd = func(msg ErrorMsg) {
	flag = msg.Err
}

var errHd2 = func(msg ErrorMsg) {
	flag = errors.New("custom error")
}

func TestStage_RunNormal(t *testing.T) {
	s := NewStage("", normal)
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	m1 := newM(nil)
	s.inputCh <- m1
	o := <-s.outputCh
	if m1 != o {
		t.FailNow()
	}
}

func TestStage_RunErrHandler(t *testing.T) {
	s := NewStage("", retErr, WithErrHandler(errHd))
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	m1 := newM(nil)
	s.inputCh <- m1
	time.Sleep(time.Millisecond)
	if flag == nil || flag.Error() != "i am an error" {
		t.FailNow()
	}
	t.Run("change errHandler", func(t *testing.T) {
		s.SetErrHandler(errHd2)
		s.inputCh <- m1
		time.Sleep(time.Millisecond)
		if flag == nil || flag.Error() != "custom error" {
			t.FailNow()
		}
	})
}

func TestStage_RunMultiWorkers(t *testing.T) {
	s := NewStage("", normal, WithWorkerNum(10))
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	time.Sleep(time.Millisecond)
	if s.GetWorkerNum() != 10 {
		t.FailNow()
	}
	for _, w := range s.workers {
		if !w.runningFlag.Val() {
			t.FailNow()
		}
	}
}

func TestStage_ResizeWorkers(t *testing.T) {
	s := NewStage("", normal)
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	s.ResizeWorkerNum(100)
	if s.GetWorkerNum() != 100 {
		t.FailNow()
	}
	time.Sleep(time.Millisecond)
	for _, w := range s.workers {
		if !w.runningFlag.Val() {
			t.FailNow()
		}
	}
	toD := s.workers[:90]
	s.ResizeWorkerNum(10)
	if s.GetWorkerNum() != 10 {
		t.FailNow()
	}
	for _, w := range toD {
		if w.runningFlag.Val() {
			t.FailNow()
		}
	}
}

func TestStage_RunTimeout(t *testing.T) {
	s := NewStage("", sleep1, WithTimeout(time.Millisecond), WithErrHandler(errHd))
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	m1 := newM(nil)
	s.inputCh <- m1
	time.Sleep(time.Second)
	if flag == nil || flag != context.DeadlineExceeded {
		t.FailNow()
	}
	t.Run("change timeout", func(t *testing.T) {
		s.SetTimeout(0)
		s.inputCh <- m1
		o := <-s.outputCh
		if m1 != o {
			t.FailNow()
		}
	})
}

func TestStage_RunChangeFunc(t *testing.T) {
	s := NewStage("", normal)
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	s.SetFunc(sleep1)
	m1 := newM(nil)
	at := time.Now()
	s.inputCh <- m1
	<-s.outputCh
	if time.Now().Sub(at).Seconds() < 1 {
		t.FailNow()
	}
}

func TestStage_returnIfIdle(t *testing.T) {
	s := NewStage("", sleep1)
	s.inputCh, s.outputCh = make(chan *M), make(chan *M)
	s.initWorkers()
	s.listen()
	m1 := newM(nil)
	at := time.Now()
	s.inputCh <- m1
	go func() {
		<-s.outputCh
	}()
	s.returnIfIdle()
	if time.Now().Sub(at).Seconds() < 1 {
		t.FailNow()
	}
}
