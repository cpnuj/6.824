package mr

import "os"
import "log"
import "net"
// import "fmt"
import "time"
import "net/rpc"
import "net/http"

// Master workflow stage
const (
	STAGE_MAP int = iota
	STAGE_REDUCE
	STAGE_DONE
)

type Master struct {
	// handle task map
	files []string
	files_cpy []string
	stat_map []TaskState
	remain_map int
	// hanld task reduce
	reduceNum int
	nReduce int
	stat_reduce []TaskState
	remain_reduce int
	// master work stage
	stage int
}

// Task state
const (
	TASK_PENDING int  = iota
	TASK_RUNNING
	TASK_FINISH
)

type TaskState struct {
	state int
	timestamp time.Time
}

// Your code here -- RPC handlers for the worker to call.

type TaskInfo struct {
	Filenames []string
	NReduce int
}

func (m *Master) InitWorker(pid *int, taskinfo *TaskInfo) error {
	taskinfo.NReduce = m.nReduce

	taskinfo.Filenames = make([]string, len(m.files_cpy))
	for i, _ := range m.files_cpy {
		taskinfo.Filenames[i] = m.files_cpy[i]
	}

	return nil
}

const (
	NO_TASK     = -1
	MAP_DONE    = -2
	REDUCE_DONE = -3
)

func (m *Master) MapTask(pid *int, tasknum *int) error {
	//	fmt.Printf("process %d, call MapTask\n", *pid)
	if m.stage != STAGE_MAP {
		*tasknum = MAP_DONE
		return nil
	}
	if len(m.files) != 0 {
		l := len(m.files)
		*tasknum = l - 1
		m.stat_map[l - 1].state = TASK_RUNNING
		m.stat_map[l - 1].timestamp = time.Now()
		m.files = m.files[:l - 1]
		// fmt.Printf("process %d take task %d\n", *pid, *tasknum)
		return nil
	}
	for i, _ := range m.stat_map {
		if ( m.stat_map[i].state != TASK_FINISH &&
		time.Now().Sub(m.stat_map[i].timestamp) > 10 * time.Second ) {
			*tasknum = i
			m.stat_map[i].timestamp = time.Now()
			// fmt.Printf("process %d take task %d\n", *pid, *tasknum)
			return nil
		}
	}
	*tasknum = NO_TASK
	return nil
}

func (m *Master) ReduceTask(pid *int, tasknum *int) error {
	// 	fmt.Printf("process %d, call ReduceTask\n", *pid)
	if m.stage != STAGE_REDUCE {
		*tasknum = REDUCE_DONE
		return nil
	}
	if m.reduceNum < m.nReduce {
		*tasknum = m.reduceNum
		m.stat_reduce[m.reduceNum].state = TASK_RUNNING
		m.stat_reduce[m.reduceNum].timestamp = time.Now()
		m.reduceNum++
		// fmt.Printf("process %d take reduce task %d\n", *pid, *tasknum)
		return nil
	}
	for i, _ := range m.stat_reduce {
		if ( m.stat_reduce[i].state != TASK_FINISH &&
		time.Now().Sub(m.stat_reduce[i].timestamp) > 10 * time.Second ) {
			*tasknum = i
			m.stat_reduce[i].timestamp = time.Now()
			// fmt.Printf("process %d take reduce task %d\n", *pid, *tasknum)
			return nil
		}
	}
	*tasknum = NO_TASK
	return nil
}

const (
	OK    = 1
	ABORT = -1
)

func (m *Master) FinishMap(tasknum *int, response *int) error {
	// fmt.Printf("map task %d finish\n", *tasknum)
	if m.stat_map[*tasknum].state == TASK_FINISH {
		// fmt.Printf("map task %d is finish by other worker\n", *tasknum)
		*response = ABORT
		return nil
	}
	m.stat_map[*tasknum].state = TASK_FINISH
	*response = OK
	m.remain_map--
	// fmt.Printf("remain map task %d\n", m.remain_map)
	if m.remain_map <= 0 {
		m.stage = STAGE_REDUCE
	}
	return nil
}

func (m *Master) FinishReduce(tasknum *int, response *int) error {
	if m.stat_reduce[*tasknum].state == TASK_FINISH {
		*response = ABORT
		return nil
	}
	m.stat_reduce[*tasknum].state = TASK_FINISH
	*response = OK
	m.remain_reduce--
	// fmt.Printf("remain reduce task %d\n", m.remain_map)
	if m.remain_reduce <= 0 {
		m.stage = STAGE_DONE
	}
	return nil
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
	// Your code here.
	return m.stage == STAGE_DONE
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

	m.files, m.files_cpy = files[:], files[:]
	m.nReduce, m.reduceNum = nReduce, 0
	m.stat_map, m.stat_reduce = make([]TaskState, len(files)), make([]TaskState, nReduce)
	m.remain_map, m.remain_reduce = len(files), nReduce
	m.stage = STAGE_MAP
	
	m.server()
	return &m
}
