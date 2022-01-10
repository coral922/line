## Line

### What is Line?
 - A code model similar to pipeline execution. **LINE** contains a series of **STAGES** and executes them in sequence.
 - Each **STAGE** could contain several **WORKERS**, which wait for inputs, execute user-defined function in separated goroutines and output to the next **STAGE**.