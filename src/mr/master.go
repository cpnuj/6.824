package mr

import "os"
import "log"
import "net"
import "fmt"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	files []string
	nReduce int
	mCnt int
	remain int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) InitTask(args *MapArgs, reply *TaskInfo) error {
	reply.NReduce, reply.M = m.nReduce, m.mCnt
	m.mCnt++
	return nil
}

func (m *Master) FinishTask(args *MapArgs, reply *TaskInfo) error {
	m.remain--
	return nil
}

func (m *Master) MapTask(args *MapArgs, reply *string) error {
	if len(m.files) == 0 {
		*reply = ""
		return nil
	}
	*reply = m.files[0]
	m.files = m.files[1:]
	fmt.Printf("%d %s\n", args.Pid, *reply)
	return nil
}

func (m *Master) ReduceTask(args *ReduceArgs, reply *int) error {
	if m.remain > 0 {
		*reply = -2
	}
	if m.nReduce <= 0 {
		*reply = -1
	}
	*reply = m.nReduce
	m.nReduce--
	return  nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	// mapf, reducef := loadPlugin(os.Args[1])
	m := Master{}
	m.files, m.nReduce, m.mCnt, m.remain = files[:], nReduce, 1, len(files)
	
	m.server()
	return &m
}
