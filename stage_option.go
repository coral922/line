package coral

import (
	"fmt"
	"time"
)

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
