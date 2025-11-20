package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTask struct {
	WorkerID int
}
type ReportTask struct {
	TaskType string // "Map" or "Reduce"
	TaskID   int
	Success  bool
	//WorkerID int
}
type ReportReply struct {
	Ack bool
}

type Reply struct {
	TaskType   string // "Map","Reduce","Wait","Exit"
	TaskID     int
	InputFiles []string
	NMap       int
	NReduce    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
