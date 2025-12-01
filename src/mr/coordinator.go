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
	mu           sync.Mutex
	workAdresses map[int]string
	mapTcount    int
	filesWnames  map[string]string // mr-<m>-<r> to original filename
}
type Task struct {
	File      string // for map tasks
	TaskID    int    // map or reduce index
	Type      string // "Map" or "Reduce"
	ReduceIdx int    // for reduce tasks
	WorkerID  int
	StartTime time.Time
}

// gets task request from worker and reply with task
func (c *Coordinator) TaskResponse(args *RequestTask, reply *Reply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf(args.Adress)
	// if we dont have this workers IPv4 address yet, store it
	if _, exists := c.workAdresses[args.WorkerID]; !exists {
		c.workAdresses[args.WorkerID] = args.Adress
		log.Printf("Registered worker %d with address %s\n", args.WorkerID, args.Adress)
	}

	println("Worker", args.WorkerID, "requested task")
	// flag to se if an task got assigned or not
	flag := false
	//if we have map tasks
	if len(c.mapTasks) > 0 {
		for i := range c.mapTasks {
			// check if the task has been assigned or if it has timed out
			if c.mapTasks[i].WorkerID == -1 || time.Since(c.mapTasks[i].StartTime) > 10*time.Second {
				c.mapTasks[i].WorkerID = args.WorkerID
				reply.TaskType = c.mapTasks[i].Type
				reply.TaskID = c.mapTasks[i].TaskID
				reply.InputFiles = c.mapTasks[i].File
				reply.NMap = len(c.files)
				reply.NReduce = len(c.reduceTasks)
				// reads the needed file for the task
				f, err := os.ReadFile(c.mapTasks[i].File)
				if err != nil {
					log.Fatalf("cannot read %v", c.mapTasks[i].File)
				}
				//adds it to the reply
				reply.File = f
				//task found flag set to true
				flag = true
				c.mapTasks[i].StartTime = time.Now()
				break
			}
		}
	} else {
		//for all reduce tasks
		for i := range c.reduceTasks {
			// check if the task has been assigned or if it has timed out
			if c.reduceTasks[i].WorkerID == -1 || time.Since(c.reduceTasks[i].StartTime) > 10*time.Second {
				c.reduceTasks[i].WorkerID = args.WorkerID
				reply.TaskType = c.reduceTasks[i].Type
				reply.TaskID = c.reduceTasks[i].TaskID
				reply.InputFiles = c.reduceTasks[i].File
				reply.NMap = len(c.files)
				reply.NReduce = len(c.reduceTasks)
				reply.ReduceIdx = c.reduceTasks[i].ReduceIdx
				list := []string{}
				// gather all worker addresses except the requesting worker
				for _, addr := range c.workAdresses {
					if addr != c.workAdresses[args.WorkerID] {
						list = append(list, addr)
					}
				}
				//forward the adresses of other workers to the reduce worker
				reply.NeededAdress = list
				//task found flag set to true
				flag = true
				c.reduceTasks[i].StartTime = time.Now()
				break
			}
		}
	}
	// if no task found, reply with wait or exit
	if !flag {
		reply.TaskType = "Wait"
		if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 {
			reply.TaskType = "Exit"
		}
	}
	return nil
}

// awnser to report of missing map file from worker
func (c *Coordinator) ReportMissingMapFile(args *ReportMissingMapFile, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Ack = false
	log.Printf("Received missing map file report: %s\n", args.MissingFile)
	// filename expected to look like mr-0-0
	parts := strings.Split(args.MissingFile, "-")

	mID, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Printf("bad map id: %v", err)
	}

	filename := c.filesWnames[args.MissingFile]
	log.Printf("Original filename for missing file %s is %s\n", args.MissingFile, filename)

	if filename != "" {
		// found the original map file
		reply.Ack = true
		// create a new map task for the missing file
		Task := Task{
			File:      filename,
			TaskID:    mID,
			Type:      "Map",
			WorkerID:  -1,
			StartTime: time.Time{},
		}
		//add to the list of map tasks
		c.mapTasks = append(c.mapTasks, Task)
		log.Printf("Re-added map task for file: %s\n", filename)
	}
	return nil
}

// awnser to report of completed task from worker
func (c *Coordinator) Report(args *ReportTask, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Ack = true
	//flag to se if we found the task
	flag := false

	//if task was successful remove it from the list
	if args.Success {
		if args.TaskType == "Map" {
			for i := range c.mapTasks {
				if c.mapTasks[i].TaskID == args.TaskID {
					c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
					for _, fname := range args.FileName {
						c.filesWnames[fname] = args.Filefrom
					}
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
		//if task not sucessful, could add timer and so on, but do nothing for now
		flag = true
	}
	if !flag {
		log.Printf("Received report for unknown task %s %d\n", args.TaskType, args.TaskID)
	}

	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
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
	//creates all task from the input files
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
	//create the cordinator with the tasks
	c := Coordinator{
		files:        files,
		mapTasks:     mapTasks,
		reduceTasks:  reduceTasks,
		mu:           sync.Mutex{},
		mapTcount:    len(mapTasks),
		filesWnames:  make(map[string]string),
		workAdresses: make(map[int]string),
	}

	c.server()
	return &c
}
