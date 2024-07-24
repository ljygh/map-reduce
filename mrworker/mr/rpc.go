package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.

type Task struct {
	TaskType int // 0 for map, 1 for reduce, 2 for no task available
	TaskID   int
	Files    []string
	NReduce  int
}

type NoticeArgs struct {
	WorkerAddr string
	TaskID     int
	TaskType   int
	Files      []string
}
