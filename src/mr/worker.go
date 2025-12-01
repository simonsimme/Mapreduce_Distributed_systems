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

var cordinatorAddress = "44.220.249.99"
var cordinatorPort = ":1234"

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

// empty struct to register RPC methods on
type WorkerO struct {
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.Printf("Worker %d starting\n", os.Getpid())

	// Register RPC server to serve fetch requests from other workers
	WorkerO := new(WorkerO)
	rpc.Register(WorkerO)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":1235")

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Printf("Worker %d RPC server started\n", os.Getpid())

	// main loop to request tasks
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
	ok := call("Coordinator.ReportMissingMapFile", &report, &reply, cordinatorAddress, cordinatorPort)
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

	ok := call("Coordinator.TaskResponse", &request, &reply, cordinatorAddress, cordinatorPort)
	if ok {
		fmt.Printf("reply %v\n", reply.TaskType) // got task
		switch reply.TaskType {
		case "Map":
			filesmade := handle_map(mapf, reply.InputFiles, reply.TaskID, reply.NReduce, reply.File)
			callReportmap(mapf, reducef, "Map", reply.TaskID, reply.InputFiles, filesmade, true, 0)
		case "Reduce":
			if handle_reduce(reducef, reply.InputFiles, reply.ReduceIdx, reply.NMap, reply.NeededAdress) {
				callReport(mapf, reducef, "Reduce", reply.TaskID, true, 0)
			} else {
				callForTask(mapf, reducef)
			}
		case "Wait":
			time.Sleep(time.Second)
			callForTask(mapf, reducef)
		case "Exit":
			return true
		default:
			log.Fatalf("Unknown task type %s", reply.TaskType)
		}

	} else {
		fmt.Printf("call failed! CORDINATOR NOT AWNSERING\n")
	}
	return false
}

func handle_reduce(reducef func(string, []string) string, filename1 string, reduceIdx int, nMap int, mapWorkerAddrs []string) bool {
	intermediate := make(map[string][]string)
	missingfiles_Flag := false

	//Check if it has all files it needs aswell as fetch missing ones
	for m := 0; m < nMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reduceIdx)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			// File does not exist locally, fetch from remote workers
			for _, addr := range mapWorkerAddrs {
				log.Printf("Attempting to fetch missing file %s from worker %s\n", filename, addr)
				// Try to get the file from this worker, true = success
				succ := false
				if !missingfiles_Flag {
					succ = getFilesFromOtherWorker(filename, addr, ":1235")

				}
				if succ {
					log.Printf("Successfully fetched missing file %s from worker %s\n", filename, addr)
					break // Stop after first successful fetch and resumes to REDUCE TASK
				} else {
					log.Printf("Failed to fetch missing file %s from worker %s\n", filename, addr)
					//if faild call cordinator to report missing file
					callForMissingMapFiles(filename, 0)
					missingfiles_Flag = true // retun false to stop the reduce task, the timer will make cordinator give task again
				}
			}
		}
	}
	if missingfiles_Flag {
		return false
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

	// Sort keys for deterministic output
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
	return true
}

// send a ReportTask RPC to the coordinator to report task completion
func callReport(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, taskType string, taskID int, success bool, counter int) {
	// retry up to 3 times to contact cordinator
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

	ok := call("Coordinator.Report", &report, &reply, cordinatorAddress, cordinatorPort)
	if ok {
		fmt.Printf("report ack %v\n", reply.Ack)
		if reply.Ack != false {
			callForTask(mapf, reducef)
		} else {
			callReport(mapf, reducef, taskType, taskID, success, counter+1)
		}
	} else {
		fmt.Printf("call failed! FAILED TO CONTACT CORDINATOR\n")
	}
}
func callReportmap(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, taskType string, taskID int, filefrom string, files []string, success bool, counter int) {
	// retry up to 3 times to contact cordinator
	if counter > 3 {
		log.Fatalf("Failed to report task %s %d after %d attempts", taskType, taskID, counter)
		return
	}
	report := ReportTask{}
	report.TaskType = taskType
	report.TaskID = taskID
	report.Success = success
	report.FileName = files
	report.Filefrom = filefrom
	//report.WorkerID = os.Getpid()

	reply := ReportReply{}

	ok := call("Coordinator.Report", &report, &reply, cordinatorAddress, cordinatorPort)
	if ok {
		fmt.Printf("report ack %v\n", reply.Ack)
		if reply.Ack != false {
			callForTask(mapf, reducef)
		} else {
			callReport(mapf, reducef, taskType, taskID, success, counter+1)
		}
	} else {
		fmt.Printf("call failed! FAILED TO CONTACT CORDINATOR\n")
	}
}

func handle_map(mapf func(string, string) []KeyValue, filename string, taskID int, nReduce int, file []byte) []string {

	// create a temporary file to hold the input data
	kva := mapf(filename, string(file))

	// partition kva into nReduce intermediate files
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}
	listofFiles := []string{}
	// write each bucket to a separate intermediate file
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", taskID, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			enc.Encode(&kv)
		}
		listofFiles = append(listofFiles, oname)
		ofile.Close()
	}
	return listofFiles
}

// call an adress with RPC
func call(rpcname string, args interface{}, reply interface{}, addr string, port string) bool {
	c, err := rpc.DialHTTP("tcp", addr+port)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// advanced part

// RPC call i need this file name
type FetchFilesArgs struct {
	Pattern string
}

// filename and its content
type FetchFilesReply struct {
	Files map[string][]byte
}

// fetch files matching pattern from another worker, if return false it failed
func getFilesFromOtherWorker(pattern string, workerAddr string, port string) bool {
	args := FetchFilesArgs{Pattern: pattern}
	reply := FetchFilesReply{}

	log.Printf("Fetching files matching pattern %s from worker %s\n", pattern, workerAddr)

	maxRetries := 3
	//attempts thre times to fetch files
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ok := call("WorkerO.FetchFiles", &args, &reply, workerAddr, port)
		log.Printf("Fetch attempt %d for pattern %s from worker %s\n", attempt, pattern, workerAddr)
		if ok {
			for fname, data := range reply.Files {
				os.WriteFile(fname, data, 0644)
			}
			return true
		} // wait longer each attempt
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	log.Printf("Failed to fetch files from worker %s after %d attempts", workerAddr, maxRetries)
	return false
}

// RPC method, replies with files matching pattern
func (w *WorkerO) FetchFiles(args *FetchFilesArgs, reply *FetchFilesReply) error {
	reply.Files = make(map[string][]byte)
	//find files matching pattern
	files, err := filepath.Glob(args.Pattern)
	if err != nil {
		return err
	}
	//search all matches and write to reply
	for _, fname := range files {
		data, err := os.ReadFile(fname)
		if err != nil {
			continue
		}
		reply.Files[fname] = data
	}
	return nil
}

// standard get IPv4
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
