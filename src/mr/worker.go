package mr

import "os"
import "fmt"
import "log"
import "time"
import "sort"
import "bufio"
import "strconv"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "path/filepath"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	pid, taskinfo := os.Getpid(), TaskInfo{}
	call("Master.InitWorker", &pid, &taskinfo)

	HandleMap(mapf, pid, &taskinfo)

	HandleReduce(reducef, pid, &taskinfo)

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func ReadContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func WriteBuckets(kvs []KeyValue, nReduce int, tasknum int) []string {
	buffer, tempfiles := make([]string, nReduce), make([]string, nReduce)
	for _, kv := range kvs {
		n := ihash(kv.Key) % nReduce
		tmpstr := fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
		buffer[n] = buffer[n] + tmpstr
	}
	for n, s := range buffer {
		pattern := "mr-out-" + strconv.Itoa(n) + "-" + strconv.Itoa(tasknum) + "-"
		ofile, err := ioutil.TempFile("./", pattern)
		if err != nil {
			log.Fatalf("cannot create temp file has pattern %s", pattern)
		}
		tempfiles[n] = ofile.Name()
		fmt.Fprintf(ofile, s)
		ofile.Close()
	}
	return tempfiles
}

func ReadBuckets(tasknum int) ([]KeyValue, []string) {
	pattern := "mr-out-" + strconv.Itoa(tasknum) + "-*"
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("cannot open file %s" + pattern)
	}

	kvs := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		reader := bufio.NewReader(file)
		for {
			var key, value string
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			fmt.Sscanf(line, "%s %s", &key, &value)
			kv := KeyValue{key, value}
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kvs))

	return kvs, files
}

func ClearBuckets(files []string) {
	for _, filename := range files {
		os.Remove(filename)
	}
}

func HandleMap(mapf func(string, string) []KeyValue, pid int, taskinfo *TaskInfo) error {
	var tasknum, response int
	nReduce := taskinfo.NReduce
	for {
		call("Master.MapTask", &pid, &tasknum)
		if tasknum == MAP_DONE {
			return nil
		}
		if tasknum == NO_TASK {
			time.Sleep(5 * time.Second)
			continue
		}

		filename := taskinfo.Filenames[tasknum]
		content := ReadContent(filename)
		kvs := mapf(filename, content)

		tempfiles := WriteBuckets(kvs, nReduce, tasknum)

		call("Master.FinishMap", &tasknum, &response)
		if response == ABORT {
			for _, filename := range tempfiles {
				os.Remove(filename)
			}
		}
	}
}

func HandleReduce(reducef func(string, []string) string, pid int, taskinfo *TaskInfo) error {
	var tasknum, response int
	for {
		call("Master.ReduceTask", &pid, &tasknum)
		if tasknum == REDUCE_DONE {
			return nil
		}
		if tasknum == NO_TASK {
			time.Sleep(10 * time.Second)
			continue
		}

		kvs, files := ReadBuckets(tasknum)

		//
		// call Reduce on each distinct key in kvs[],
		// and print the result to mr-out-tasknum.
		//

		tmpfile, _ := ioutil.TempFile("./", "mr-out-"+strconv.Itoa(tasknum)+"-")
		i := 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := reducef(kvs[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpfile, "%v %v\n", kvs[i].Key, output)

			i = j
		}

		call("Master.FinishReduce", &tasknum, &response)
		if response == OK {
			ClearBuckets(files)
			os.Rename(tmpfile.Name(), "mr-out-"+strconv.Itoa(tasknum))
		} else {
			os.Remove(tmpfile.Name())
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
