package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		go func(taskNum int, nios int, phase jobPhase) {
			var doTaskArgs DoTaskArgs
			doTaskArgs.JobName = mr.jobName
			doTaskArgs.TaskNumber = taskNum
			doTaskArgs.Phase = phase
			doTaskArgs.File = mr.files[taskNum]
			doTaskArgs.NumOtherPhase = nios

			for {
				worker := <-mr.registerChannel
				ok := call(worker, "Worker.DoTask", &doTaskArgs, new(struct{}))
				// if worker fails while handing an rpc from master.
				// the call() will eventually return false due to timeout
				if ok {
					wg.Done()
					mr.registerChannel <- worker
					break
				} else {
					fmt.Println("fail to dotask from master to worker.")
				}
				// else 表示失败, 使用新的worker 则会进入下一次for循环重试
			}
		}(i, nios, phase)
	}
	wg.Wait()	// wait all task finished.
	fmt.Printf("Schedule: %v phase done\n", phase)
}
