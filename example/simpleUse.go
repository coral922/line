// +build ignore

package main

import (
	"fmt"
	"line"
)

func main() {
	// A simple example that fills object's attr and print it.
	l := line.New()
	stages := []*line.Stage{
		line.NewStage("stage1", stageFillS1, line.WithWorkerNum(100)),
		line.NewStage("stage2", stageFillS2, line.WithWorkerNum(100)),
		line.NewStage("stage3", stageFillS3, line.WithWorkerNum(100)),
		line.NewStage("stage4", stagePrint, line.WithWorkerNum(100)),
	}
	inputs := make([]*object, 0)
	for i := 0; i < 100; i++ {
		inputs = append(inputs, &object{h: "low"})
	}
	l.SetStages(stages).Run()
	l.InputAndWait(inputs, line.WithBatch())
}

type object struct {
	s1, s2, s3 string
	h          string
}

func stageFillS1(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	input.Item().(*object).s1 = "S1"
	return input, nil
}

func stageFillS2(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	input.Item().(*object).s2 = "S2"
	return input, nil
}

func stageFillS3(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	input.Item().(*object).s3 = "S3"
	return input, nil
}

func stagePrint(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
	fmt.Println(*(input.Item().(*object)))
	return input, nil
}
