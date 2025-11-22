package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		if !callForTask(mapf, reducef) {
			break
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func callForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	request := RequestTask{}
	request.WorkerID = os.Getpid()

	reply := Reply{}

	ok := call("Coordinator.TaskResponse", &request, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply.TaskType) // got task
		switch reply.TaskType {
		case "Map":
			handle_map(mapf, reply.InputFiles, reply.TaskID, reply.NReduce)
			callReport(mapf, reducef, "Map", reply.TaskID, true, 0)
		case "Reduce":
			handle_reduce(reducef, reply.ReduceIdx, reply.NMap)
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

func handle_reduce(reducef func(string, []string) string, reduceIdx int, nMap int) {
	intermediate := make(map[string][]string)

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
func handle_map(mapf func(string, string) []KeyValue, filename string, taskID int, nReduce int) {
	// read input file
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
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
