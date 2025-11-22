package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	files       []string
	mapTasks    []Task
	reduceTasks []Task
}
type Task struct {
	File      string // for map tasks
	TaskID    int    // map or reduce index
	Type      string // "Map" or "Reduce"
	ReduceIdx int    // for reduce tasks
	WorkerID  int
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) TaskResponse(args *RequestTask, reply *Reply) error {
	println("Worker", args.WorkerID, "requested task")
	flag := false
	if len(c.mapTasks) > 0 {
		for i := range c.mapTasks {
			if c.mapTasks[i].WorkerID == -1 || time.Since(c.mapTasks[i].StartTime) > 10*time.Second {
				c.mapTasks[i].WorkerID = args.WorkerID
				reply.TaskType = c.mapTasks[i].Type
				reply.TaskID = c.mapTasks[i].TaskID
				reply.InputFiles = c.mapTasks[i].File
				reply.NMap = len(c.files)
				reply.NReduce = len(c.reduceTasks)
				flag = true
				c.mapTasks[i].StartTime = time.Now()
				break
			}
		}
	} else {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].WorkerID == -1 || time.Since(c.reduceTasks[i].StartTime) > 10*time.Second {
				c.reduceTasks[i].WorkerID = args.WorkerID
				reply.TaskType = c.reduceTasks[i].Type
				reply.TaskID = c.reduceTasks[i].TaskID
				reply.InputFiles = c.reduceTasks[i].File
				reply.NMap = len(c.files)
				reply.NReduce = len(c.reduceTasks)
				reply.ReduceIdx = c.reduceTasks[i].ReduceIdx
				flag = true
				c.reduceTasks[i].StartTime = time.Now()
				break
			}
		}
	}

	if !flag {
		reply.TaskType = "Wait"
		if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 {
			reply.TaskType = "Exit"
		}
	}
	return nil

}

func (c *Coordinator) Report(args *ReportTask, reply *ReportReply) error {
	reply.Ack = true
	flag := false

	if args.Success {
		if args.TaskType == "Map" {
			for i := range c.mapTasks {
				if c.mapTasks[i].TaskID == args.TaskID {
					c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
					flag = true
					return nil
				}
			}
		} else if args.TaskType == "Reduce" {
			for i := range c.reduceTasks {
				if c.reduceTasks[i].TaskID == args.TaskID {
					c.reduceTasks = append(c.reduceTasks[:i], c.reduceTasks[i+1:]...)
					flag = true
					return nil
				}
			}
		}

	} else {
		flag = true
	}
	if !flag {
		// task not found or compleated
	}

	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return len(c.mapTasks) == 0 && len(c.reduceTasks) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	id := 0
	mapTasks := make([]Task, len(files))
	for i, fname := range files {
		mapTasks[i] = Task{
			File:      fname,
			TaskID:    id,
			Type:      "Map",
			WorkerID:  -1,
			StartTime: time.Time{},
		}
		id++
	}
	reduceTasks := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = Task{
			TaskID:    id,
			Type:      "Reduce",
			ReduceIdx: i,
			WorkerID:  -1,
			StartTime: time.Time{},
		}
		id++
	}
	println("Coordinator created with", len(mapTasks), "map tasks and", len(reduceTasks), "reduce tasks.")
	c := Coordinator{
		files:       files,
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
	}

	c.server()
	return &c
}
