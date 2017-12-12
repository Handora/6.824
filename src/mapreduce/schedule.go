package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// the current jobs needed to be sent
	var i int
	var wg sync.WaitGroup

	for v := range registerChan {
		if i == ntasks {
			break
		}
		v := v
		var tArgs DoTaskArgs
		tArgs.TaskNumber = i
		tArgs.File = mapFiles[i]
		i++
		tArgs.JobName = jobName
		tArgs.Phase = phase
		tArgs.NumOtherPhase = n_other

		go func() {
			wg.Add(1)
			ok := call(v, "Worker.DoTask", &tArgs, nil)
			if ok != true {
				fmt.Printf("Error when remote call worker %v", v)
			}
			wg.Done()
			registerChan <- v
		}()
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
