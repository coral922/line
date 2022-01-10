package main

import (
	"fmt"
	"line"
	"math/rand"
	"time"
)

func main() {
	l := line.NewLine(line.WithMaxQueueLen(10000))
	stages := []*line.Stage{
		line.NewStage("stage1", stageFillS1, line.WithWorkerNum(2)),
		line.NewStage("stage2", stageFillS2, line.WithWorkerNum(5)),
		line.NewStage("stage3", stageFillS3, line.WithWorkerNum(10)),
		line.NewStage("stage4", stagePrint, line.WithWorkerNum(1)),
	}
	l.SetStages(stages...).Run()
	inputs := make([]*object, 0)
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, &object{})
	}
	t := time.Now()
	l.InputAndWait(inputs, line.WithBatch())
	fmt.Println("\n\n\n time spent :", time.Now().Sub(t).String())
}

type object struct {
	s1, s2, s3 string
}

func stageFillS1(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	randomSleepS(1)
	input.Item().(*object).s1 = "S1"
	return input, nil
}

func stageFillS2(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	randomSleepS(2)
	input.Item().(*object).s2 = "S2"

	return input, nil
}

func stageFillS3(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	randomSleepS(3)
	input.Item().(*object).s3 = "S3"

	return input, nil
}

func stagePrint(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	fmt.Println(*(input.Item().(*object)))
	return input, nil
}

func randomSleepS(max int) {
	i := rand.Intn(max * 1000)
	time.Sleep(time.Duration(i * int(time.Millisecond)))
}
