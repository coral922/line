package line

import (
	"errors"
	"sync/atomic"
)

type WorkFunc func(ctx ExecContext, input *M) (output *M, err error)

type ErrHandler func(msg ErrorMsg)

type sBool struct {
	value int32
}

func newSBool(value ...bool) *sBool {
	t := &sBool{}
	if len(value) > 0 {
		if value[0] {
			t.value = 1
		} else {
			t.value = 0
		}
	}
	return t
}

func (v *sBool) Set(value bool) (old bool) {
	if value {
		old = atomic.SwapInt32(&v.value, 1) == 1
	} else {
		old = atomic.SwapInt32(&v.value, 0) == 1
	}
	return
}

func (v *sBool) Val() bool {
	return atomic.LoadInt32(&v.value) > 0
}

func (v *sBool) Cas(old, new bool) (swapped bool) {
	var oldInt32, newInt32 int32
	if old {
		oldInt32 = 1
	}
	if new {
		newInt32 = 1
	}
	return atomic.CompareAndSwapInt32(&v.value, oldInt32, newInt32)
}

var (
	ErrNoStage     = errors.New("There is no stage. ")
	ErrAlreadyOpen = errors.New("Line is already open. ")
	ErrDupName     = errors.New("Stage name already exist. ")
)
