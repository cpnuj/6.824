package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "fmt"
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

// Add your RPC definitions here.

type TaskInfo struct {
	NReduce, M int
	// m -- worker number
}

type MapArgs struct {
	Pid int
}

type MapReply struct {
	filename, content string
	finish bool
}

type ReduceArgs struct {

}

type ReduceReply struct {

}

func debug(s interface{}) {
	fmt.Println(s)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
