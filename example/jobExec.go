// +build ignore

package main

import (
	"fmt"
	"line"
	"math/rand"
	"strconv"
	"time"
)

const (
	MaxWaitJobNum    = 100
	DefaultWorkerNum = 10
	DefaultTimeout   = 1800 * time.Millisecond
)

//struct that transmit in stages
type Job struct {
	ID        string
	StartAt   time.Time
	FinishAt  time.Time
	Param     map[string]interface{}
	Result    interface{}
	Spend     map[string]time.Duration
	ErrReason string
}

func (j *Job) Save() error {
	//do sth to save job's information.
	//for example update it in mysql.
	//fmt.Println(*j)
	return nil
}

func StagePrepare() *line.Stage {
	return line.NewStage("prepare", func(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
		job := input.Item().(*Job)
		job.StartAt = time.Now()
		//you can handle with job's param
		job.Param["foo"] = "bar"
		return input, nil
	})
}

func StageLast() *line.Stage {
	return line.NewStage("save", func(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
		job := input.Item().(*Job)
		job.Result = "ok"
		//save to some database
		job.FinishAt = time.Now()
		_ = job.Save()
		return input, nil
	})
}

func JobPartStage(partName string) *line.Stage {
	return line.NewStage(partName, func(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
		job := input.Item().(*Job)
		t := time.Now()
		defer func() {
			job.Spend[partName] = time.Now().Sub(t)
		}()
		//simulating executing by random sleep
		randomSleep(2)
		return input, nil
	},
		line.WithErrHandler(errHandler),
		line.WithTimeout(DefaultTimeout),
		line.WithWorkerNum(DefaultWorkerNum))
}

var errHandler line.ErrHandler = func(msg line.ErrorMsg) {
	fmt.Printf("%s stage [worker_id : %s] error: %s \n", msg.StageName, msg.WorkerID, msg.Err)
	job := msg.M.Item().(*Job)
	job.FinishAt = msg.OccurAt
	job.ErrReason = msg.Err.Error()
}

func main() {
	l := line.New(line.WithPQSupported(), line.WithMaxQueueLen(MaxWaitJobNum))
	l.SetStages([]*line.Stage{
		StagePrepare(),
		JobPartStage("partA"),
		JobPartStage("partB"),
		JobPartStage("partC"),
		JobPartStage("partD"),
		StageLast(),
	}).Run()

	//produce some job
	t := time.Now()
	jobs := GetSomeJobs(100)
	l.InputAndWait(jobs, line.WithBatch())
	fmt.Println()
	fmt.Println("finish 100 job in :", time.Now().Sub(t).String())
	for _, j := range jobs {
		fmt.Println(*j)
	}
}

func GetSomeJobs(num int) []*Job {
	res := make([]*Job, 0)
	for i := 0; i < num; i++ {
		res = append(res, &Job{
			ID:    strconv.Itoa(i),
			Param: make(map[string]interface{}),
			Spend: make(map[string]time.Duration)})
	}
	return res
}

func randomSleep(maxSecond int) {
	time.Sleep(time.Duration(rand.Intn(maxSecond*1000) * int(time.Millisecond)))
}
