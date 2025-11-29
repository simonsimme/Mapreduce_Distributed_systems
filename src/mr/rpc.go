package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type RequestTask struct {
	WorkerID int
	Adress   string
}
type ReportTask struct {
	TaskType string // "Map" or "Reduce"
	TaskID   int
	Success  bool
}
type ReportReply struct {
	Ack bool
}
type ReportMissingMapFile struct {
	MissingFile string
}

type Reply struct {
	FileName     string
	TaskType     string // "Map","Reduce","Wait","Exit"
	TaskID       int
	InputFiles   string
	NMap         int
	NReduce      int
	ReduceIdx    int
	NeededAdress []string
	File         []byte
}
