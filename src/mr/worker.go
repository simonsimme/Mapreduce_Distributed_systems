package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var cordinator_address = "3.227.0.14"
var cordinator_port = ":1234"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerO struct {
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.Printf("Worker %d starting\n", os.Getpid())
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	WorkerO := new(WorkerO)
	rpc.Register(WorkerO)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":1235")

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Printf("Worker %d RPC server started\n", os.Getpid())

	for {
		if !callForTask(mapf, reducef) {
			break
		}
	}

}

func callForMissingMapFiles(missingFile string, counter int) {
	if counter > 3 {
		log.Fatalf("Failed to report missing map files after %d attempts", counter) // cordinator died or unrechable
		return
	}
	report := ReportMissingMapFile{}
	report.MissingFile = missingFile

	reply := ReportReply{}
	ok := call("Coordinator.ReportMissingMapFiles", &report, &reply)
	if ok {
		fmt.Printf("report missing files ack %v\n", reply.Ack)
		if reply.Ack {
			return
		}
	} else {
		callForMissingMapFiles(missingFile, counter+1)
	}
}
func callForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	log.Printf("Worker %d requesting task\n", os.Getpid())
	request := RequestTask{}
	request.WorkerID = os.Getpid()
	ip, err := GetPublicIPv4()
	if err != nil {
		log.Fatalf("Failed to get public IP address: %v", err)
	}
	log.Printf("Worker %d public IP address: %s\n", os.Getpid(), ip)
	request.Adress = ip

	reply := Reply{}

	ok := call("Coordinator.TaskResponse", &request, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply.TaskType) // got task
		switch reply.TaskType {
		case "Map":
			log.Println(len(reply.File))
			handle_map(mapf, reply.InputFiles, reply.TaskID, reply.NReduce, reply.File)
			callReport(mapf, reducef, "Map", reply.TaskID, true, 0)
		case "Reduce":
			handle_reduce(reducef, reply.ReduceIdx, reply.NMap, reply.NeededAdress)
			callReport(mapf, reducef, "Reduce", reply.TaskID, true, 0)
		case "Wait":
			time.Sleep(time.Second)
			callForTask(mapf, reducef)
		case "Exit":
			return true
		default:
			log.Fatalf("Unknown task type %s", reply.TaskType)
		}

	} else {
		fmt.Printf("call failed!\n")
	}
	return false
}

func handle_reduce(reducef func(string, []string) string, reduceIdx int, nMap int, mapWorkerAddrs []string) {
	intermediate := make(map[string][]string)
	//Check if it has all files it needs aswell as fetch missing ones
	for m := 0; m < nMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reduceIdx)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			// File does not exist locally, fetch from remote workers
			for _, addr := range mapWorkerAddrs {
				log.Printf("Attempting to fetch missing file %s from worker %s\n", filename, addr)

				succ := getFilesFromOtherWorker(filename, addr)
				if succ {
					log.Printf("Successfully fetched missing file %s from worker %s\n", filename, addr)
					break // Stop after first successful fetch
				} else {
					log.Printf("Failed to fetch missing file %s from worker %s\n", filename, addr)
					callForMissingMapFiles(filename, 0)
					return
				}
			}
		}
		// Now you can safely open/process the file
	}
	log.Printf("Worker %d starting reduce task %d\n", os.Getpid(), reduceIdx)
	// Read all map output files for this reduce partition
	for m := 0; m < nMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reduceIdx)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}

		file.Close()
	}

	// Sort keys for deterministic output (optional)
	keys := make([]string, 0, len(intermediate))
	for k := range intermediate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write output
	oname := fmt.Sprintf("mr-out-%d", reduceIdx)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for _, key := range keys {
		output := reducef(key, intermediate[key])
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
}
func callReport(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, taskType string, taskID int, success bool, counter int) {
	if counter > 3 {
		log.Fatalf("Failed to report task %s %d after %d attempts", taskType, taskID, counter)
		return
	}
	report := ReportTask{}
	report.TaskType = taskType
	report.TaskID = taskID
	report.Success = success
	//report.WorkerID = os.Getpid()

	reply := ReportReply{}

	ok := call("Coordinator.Report", &report, &reply)
	if ok {
		fmt.Printf("report ack %v\n", reply.Ack)
		if reply.Ack != false {
			callForTask(mapf, reducef)
		} else {
			callReport(mapf, reducef, taskType, taskID, success, counter+1)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
}
func handle_map(mapf func(string, string) []KeyValue, filename string, taskID int, nReduce int, file []byte) {
	// read input file
	/*content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	} */
	err := os.WriteFile(filename, file, 0644)
	if err != nil {
		log.Fatalf("cannot write %v: %v", filename, err)
	}
	//content := Get_client(cordinator_address, cordinator_port, filename)
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	// call mapf
	kva := mapf(filename, string(content))

	// partition kva into nReduce intermediate files
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", taskID, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "3.227.0.14"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
func callWorker(rpcname string, args interface{}, reply interface{}, addr string) bool {
	c, err := rpc.DialHTTP("tcp", addr+":1235")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	log.Printf("Calling worker at %s for RPC %s\n", addr, rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// advanced part
type FetchFilesArgs struct {
	Pattern string
}
type FetchFilesReply struct {
	Files map[string][]byte
}

func getFilesFromOtherWorker(pattern string, workerAddr string) bool {
	args := FetchFilesArgs{Pattern: pattern}
	reply := FetchFilesReply{}
	log.Printf("Fetching files matching pattern %s from worker %s\n", pattern, workerAddr)
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ok := callWorker("WorkerO.FetchFiles", &args, &reply, workerAddr)
		log.Printf("Fetch attempt %d for pattern %s from worker %s\n", attempt, pattern, workerAddr)
		if ok {
			for fname, data := range reply.Files {
				os.WriteFile(fname, data, 0644)
			}
			return true
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	log.Printf("Failed to fetch files from worker %s after %d attempts", workerAddr, maxRetries)
	return false
}
func (w *WorkerO) FetchFiles(args *FetchFilesArgs, reply *FetchFilesReply) error {
	reply.Files = make(map[string][]byte)
	files, err := filepath.Glob(args.Pattern)
	if err != nil {
		return err
	}
	for _, fname := range files {
		data, err := os.ReadFile(fname)
		if err != nil {
			continue
		}
		reply.Files[fname] = data
	}
	return nil
}
func GetPublicIPv4() (string, error) {
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(ip), nil
}
