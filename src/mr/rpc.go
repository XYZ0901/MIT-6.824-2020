package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

const (
	NONE = 0
	MAP = 1
	REDUCE = 2
)

// Add your RPC definitions here.
type AskTaskRequest struct {
	Src string
}

type AskTaskResponse struct {
	FileNames_ []string
	InputNum_ int64
	TaskType_ int
	Id_ int
}

type SubmitTaskRequest struct {
	TaskType_ int
	Index int
}
type SubmitTaskResponse struct {
	Success bool
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	os.Getpid()
	return s
}

func workerSock() string {
	s := "/var/tmp/824-wroker-"
	s += strconv.Itoa(os.Getpid())
	return s
}