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

	// deal with map
	mapargs, taskInfo := MapArgs{}, TaskInfo{}
	call("Master.InitTask", &mapargs, &taskInfo)
	maptmp := []KeyValue{}
	for {
		var filename string
		call("Master.MapTask", &mapargs, &filename)
		if filename == "" {
			break
		}

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))
		maptmp = append(maptmp, kva...)
	}

	// write map result to bucket files for reduce
	nReduce, m := taskInfo.NReduce, taskInfo.M
	buffer := make([]string, nReduce)
	for _, kv := range maptmp {
		key, value := kv.Key, kv.Value
		n := ihash(key) % nReduce
		tmpstr := fmt.Sprintf("%v %v\n", key, value)
		buffer[n] = buffer[n] + tmpstr
	}
	for n, s := range buffer {
		if s == "" {
			continue
		}
		oname := "log-mr-" + strconv.Itoa(n) + "-" + strconv.Itoa(m)
		ofile, _ := os.Create(oname)
		fmt.Fprintf(ofile, s)
		ofile.Close()
	}

	// TODO: notice master map task is done

	// deal with reduce
	for {
		reduceargs, nTarget := ReduceArgs{}, 0
		call("Master.ReduceTask", &reduceargs, &nTarget)
		if nTarget == -1 {
			break
		}

		pattern = "log-mr-" + strconv.Itoa(nTarget) + "-*"
		files, err := filepath.Glob(pattern)
		if err != nil {
			log.Fatalf("cannot open file %s" + pattern)
		}

		// read files kv pairs into reducetmp
		reducetmp := []KeyValue{}
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
				reducetmp = append(reducetmp, kv)
			}
			file.Close()
		}

		sort.Sort(ByKey(reducetmp))
		ofile := "mr-out-" + strconv.Itoa(nTarget)

		//
		// call Reduce on each distinct key in reducetmp[],
		// and print the result to mr-out-nTarget.
		//
		i := 0
		for i < len(reducetmp) {
			j := i + 1
			for j < len(reducetmp) && reducetmp[j].Key == reducetmp[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, reducetmp[k].Value)
			}
			output := reducef(reducetmp[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", reducetmp[i].Key, output)

			i = j
		}		
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

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
