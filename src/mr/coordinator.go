package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	files        []string
	mapTasks     []Task
	reduceTasks  []Task
	mapTaskBank  []Task
	mu           sync.Mutex
	workAdresses map[int]string
}
type Task struct {
	File      string // for map tasks
	TaskID    int    // map or reduce index
	Type      string // "Map" or "Reduce"
	ReduceIdx int    // for reduce tasks
	WorkerID  int
	StartTime time.Time
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) TaskResponse(args *RequestTask, reply *Reply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf(args.Adress)
	if _, exists := c.workAdresses[args.WorkerID]; !exists {
		c.workAdresses[args.WorkerID] = args.Adress
		log.Printf("Registered worker %d with address %s\n", args.WorkerID, args.Adress)
	}
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
				f, err := os.ReadFile(c.mapTasks[i].File)
				log.Printf("Reading file %s", c.mapTasks[i].File)
				f, err = os.ReadFile(c.mapTasks[i].File)
				if err != nil {
					log.Fatalf("cannot read %v", c.mapTasks[i].File)
				}
				log.Printf("Read %d bytes from %s", len(f), c.mapTasks[i].File)
				reply.File = f
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
				list := []string{}
				for _, addr := range c.workAdresses {
					if addr != c.workAdresses[args.WorkerID] {
						list = append(list, addr)
					}
				}
				reply.NeededAdress = list
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
func (c *Coordinator) ReportMissingMapFile(args *ReportMissingMapFile, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Ack = false
	log.Printf("Received missing map file report: %s\n", args.MissingFile)
	// if we get like mr-0-0
	filename := getInputFilenameFromIntermediate(args.MissingFile, c.mapTaskBank)
	if filename != "" {
		reply.Ack = true
	}
	Task := Task{
		File:      filename,
		TaskID:    len(c.mapTasks) + 1,
		Type:      "Map",
		WorkerID:  -1,
		StartTime: time.Time{},
	}
	c.mapTasks = append(c.mapTasks, Task)
	log.Printf("Re-added map task for file: %s\n", filename)
	//
	return nil
}

func (c *Coordinator) Report(args *ReportTask, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		files:        files,
		mapTasks:     mapTasks,
		mapTaskBank:  mapTasks,
		reduceTasks:  reduceTasks,
		mu:           sync.Mutex{},
		workAdresses: make(map[int]string),
	}

	c.server()
	return &c
}
func getInputFilenameFromIntermediate(intermediate string, mapTaskBank []Task) string {
	// intermediate is like "mr-0-0"
	parts := strings.Split(intermediate, "-")
	if len(parts) < 3 {
		return ""
	}
	mapTaskID, err := strconv.Atoi(parts[1])
	if err != nil {
		return ""
	}
	for _, t := range mapTaskBank {
		if t.TaskID == mapTaskID {
			return t.File
		}
	}
	return ""
}

func getIPsforMaps() {

}
