package line

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type C struct {
	Str       string
	StrMd5    []byte
	StrSha1   []byte
	StrBase64 string
}

func newC(s string) *C {
	return &C{Str: s}
}

var md5F = func(ctx ExecContext, input *M) (output *M, err error) {
	c := input.Item().(*C)
	m := md5.New()
	_, err = m.Write([]byte(c.Str))
	if err != nil {
		return nil, err
	}
	c.StrMd5 = m.Sum(nil)
	return input, nil
}

var sha1F = func(ctx ExecContext, input *M) (output *M, err error) {
	c := input.Item().(*C)
	m := sha1.New()
	_, err = m.Write([]byte(c.Str))
	if err != nil {
		return nil, err
	}
	c.StrSha1 = m.Sum(nil)
	return input, nil
}

var b64F = func(ctx ExecContext, input *M) (output *M, err error) {
	c := input.Item().(*C)
	c.StrBase64 = base64.StdEncoding.EncodeToString([]byte(c.Str))
	return input, nil
}

var resBucket = make([]*C, 0)

var final = func(ctx ExecContext, input *M) (output *M, err error) {
	time.Sleep(time.Millisecond)
	cc := input.Item().(*C)
	if strings.Contains(cc.Str, "err") {
		return input, errors.New(cc.Str)
	}
	resBucket = append(resBucket, cc)

	return input, nil
}

func clean() {
	resBucket = make([]*C, 0)
}

func TestLine_Integrated_General(t *testing.T) {
	clean()
	l := New(WithMaxQueueLen(10000))
	stages := []*Stage{
		NewStage("md5", md5F, WithWorkerNum(10)),
		NewStage("sha1", sha1F, WithWorkerNum(10)),
		NewStage("b64", b64F, WithWorkerNum(10)),
		NewStage("final", final, WithWorkerNum(1)),
	}
	l.SetStages(stages).Run()

	//always has input
	go func() {
		for {
			time.Sleep(time.Microsecond)
			l.Input(newC(time.Now().String()))
		}
	}()

	a1 := newC("a1")
	l.InputAndWait(a1)
	if a1.StrBase64 == "" || len(a1.StrSha1) == 0 || len(a1.StrMd5) == 0 {
		t.FailNow()
	}

	l.RemoveStage("b64")
	a2 := newC("a2")
	l.InputAndWait(a2)
	if a2.StrBase64 != "" || len(a2.StrSha1) == 0 || len(a2.StrMd5) == 0 {
		t.FailNow()
	}

	l.RemoveStage("md5")
	a3 := newC("a3")
	l.InputAndWait(a3)
	if a3.StrBase64 != "" || len(a3.StrSha1) == 0 || len(a3.StrMd5) != 0 {
		t.FailNow()
	}

	l.AppendStages([]*Stage{NewStage("md5", md5F, WithWorkerNum(10))})
	a4 := newC("a4")
	l.InputAndWait(a4)
	if a4.StrBase64 != "" || len(a4.StrSha1) == 0 || len(a4.StrMd5) == 0 {
		fmt.Println(*a4)
		t.FailNow()
	}

	l.AppendStages([]*Stage{NewStage("b64", b64F, WithWorkerNum(10))}, "sha1")
	a5 := newC("a5")
	l.InputAndWait(a5)
	if a5.StrBase64 == "" || len(a5.StrSha1) == 0 || len(a5.StrMd5) == 0 {
		t.FailNow()
	}

	l.SetStages([]*Stage{
		NewStage("md5", md5F, WithWorkerNum(10)),
		NewStage("b64", b64F, WithWorkerNum(10)),
		NewStage("final", final, WithWorkerNum(1)),
	})
	a6 := newC("a6")
	l.InputAndWait(a6)
	if a6.StrBase64 == "" || len(a6.StrSha1) != 0 || len(a6.StrMd5) == 0 {
		t.FailNow()
	}
	l.AppendStages([]*Stage{NewStage("sha1", sha1F, WithWorkerNum(10))}, "b64")

	a7 := newC("err_a7")
	l.GetStage("final").SetErrHandler(func(msg ErrorMsg) {
		cc := msg.M.Item().(*C)
		if msg.Err != context.DeadlineExceeded {
			cc.Str += "_handled"
		} else {
			cc.Str += "_timeout"
		}
	})
	l.InputAndWait(a7)
	if a7.Str != "err_a7_handled" {
		t.FailNow()
	}
	l.GetStage("final").SetTimeout(time.Microsecond)
	a8 := newC("err_a8")
	l.InputAndWait(a8)
	if a8.Str != "err_a8_timeout" {
		t.FailNow()
	}
	l.GetStage("final").SetTimeout(0)
}
