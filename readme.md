# Line
[![Go Report Card](https://goreportcard.com/badge/github.com/coral922/line)](https://goreportcard.com/report/github.com/coral922/line)
[![Coverage](http://gocover.io/_badge/github.com/coral922/line)](http://gocover.io/github.com/coral922/line)
[![Go Reference](https://pkg.go.dev/badge/github.com/coral922/line.svg)](https://pkg.go.dev/github.com/coral922/line)

### What is Line?
 - A code model similar to pipeline execution. **LINE** contains a InputQueue, and a series of **STAGES** and executes them in sequence.
 - Each **STAGE** could contain several **WORKERS**, which wait for inputs, execute user-defined function in separated goroutines and output to the next **STAGE**.
 
### When to use? 
If your program has processes similar to below codes.
 ```
//fetching items
items := fetchItems()

//handle each item
for _, item := range items {
    err := doSomethingA(item)
    if err != nil {
        continue
    }
    err = doSomethingB(item)
    if err != nil {
        continue
    }
    err = doSomethingC(item)
    ...
}
```
You may consider refactoring it into below codes, for higher scalability and clarity.
```
type Func func(item Item) error
type FuncList []Func

type (fl FuncList) Handle(items []Item) {
    for _, item := range items {
        for _, stage := range fl {
            if err := stage(item); err != nil {
                break
            }
        }
    }
}

//define stages
var stages FuncList = []Func{doSomethingA, doSomethingB, doSomethingC, ...}

//fetching items
items := fetchItems()

//handle them
stages.Handle(items)
```
Further, you may want those functions run in parallel to improve performance, or want to easily adjust them. 
That's actually what Line aims to do. Line links those functions and gives you ability of management, such as add/remove. 

### How to use?
First, you need to split your process into several stages reasonably.
Then define your **object struct** which is passed between your stages and define each stage functions with specific function signature (type line.WorkFunc).
The param **input** in type *line.M has a method **Item()**, from which you can get your object.
```
// your custom struct 
type MyItem struct {
    paramA interface{}
    paramB interface{}
    ...
}

// your stage function
func YourStage1Func(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
    //you need to assert item's type.
    item := input.Item().(*MyItem)

    if err = doSomethingWithItem(item); err != nil {
        // if you return an error, the process will be interrupted and an ErrHandler will handle this error.
        return nil, err
    }

    // send to next stage, if there is no next stage, the process is done.
    return input, nil
}

func YourStage2Func(ctx line.ExecContext, input *line.M) (output *line.M, err error) {
    item := input.Item().(*MyItem)
    doSomethingElseWithItem(item)
    return input, nil
}
``` 
Create Line And Set Stages with your options, then run it.
```
l := line.New(line.WithMaxQueueLen(10), line.WithPQSupported())

//create your stages with your function and options.
stages := []*line.Stage{
        //WithWorkerNum represents how many goroutines are created to exec your stage function.
        line.NewStage("stage1Name", YourStage1Func, line.WithWorkerNum(10)),
        line.NewStage("stage2Name", YourStage2Func, line.WithWorkerNum(10)),
    }

//Set and run.
l.SetStages(stages).Run()
```
Now you can input your objects with options (such as setting priority) to the line.
```
item := &MyItem{}
// nonblock.
l.Input(item, line.WithPriority(10))

item2 := &MyItem2{}
// block until item2 been done.
l.InputAndWait(item2, line.WithPriority(20)) 

items := fetchSomeItems()
// batch input, block until all been done.
l.InputAndWait(items, line.WithBatch())
```

### Features
- Dynamic stage adjustment, enable add/delete stages while line is running.
- Dynamic resizing scale of concurrency.
- Priority input queue supported.
- Support both block and non-block input.
- Support pausing/resuming.