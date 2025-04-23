package mr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

var theWorkerId int = 0
var theMapf func(string, string) []KeyValue
var theReducef func(string, []string) string

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	theMapf = mapf
	theReducef = reducef
	if workerId, err := heartBeatReport(0); err != nil {
		fmt.Printf("worker regist failed!\n")
		os.Exit(1)
	} else {
		theWorkerId = workerId
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		flag := true
		for flag {
			select {
			case <-ctx.Done():
				fmt.Printf("worker done. stop heartbeat...")
				flag = false
			case <-ticker.C:
				heartBeatReport(theWorkerId)
			}
		}
	}(ctx)

	ticker := time.NewTicker(time.Duration(1) * time.Second)
	defer ticker.Stop()
	working := true
	for working {
		<-ticker.C
		if reply, err := getOneTask(); err != nil {
		} else {
			switch reply.TaskType {
			case MAPPING:
				if err := doMap(reply.FileName, reply.TaskIndex, reply.NReduce); err != nil {
					// map fail
				} else {
					reportTaskFinish(MAPPING, reply.TaskIndex)
				}
			case REDUCING:
				if err := doReduce(reply.TaskIndex); err != nil {
					// reduce fail
				} else {
					reportTaskFinish(REDUCING, reply.TaskIndex)
				}
			default:
				// taskType = 0
				// no job returned
			}
		}
	}

	cancel()
}

func doMap(fileName string, fileIndex int, nReduce int) error {
	imArr := make([][]KeyValue, nReduce)
	for k := range imArr {
		imArr[k] = []KeyValue{}
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := theMapf(fileName, string(content))
	for _, v := range kva {
		h := ihash(v.Key) % nReduce
		imArr[h] = append(imArr[h], v)
	}
	for k := range imArr {
		sort.Sort(ByKey(imArr[k]))
		oname := fmt.Sprintf("mr-intermediate-%d-%d", fileIndex, k)
		fhandle, err := os.CreateTemp(".", "abcXABC*")
		if err != nil {
			return err
		}
		defer fhandle.Close()

		enc := json.NewEncoder(fhandle)
		for _, vv := range imArr[k] {
			if err := enc.Encode(&vv); err != nil {
				return err
			}
		}
		if err := os.Rename(fhandle.Name(), oname); err != nil {
			return err
		}
	}
	return nil
}

func doReduce(reduceIndex int) error {
	pattern := fmt.Sprintf("mr-intermediate-*-%d", reduceIndex)
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return err
	}
	if len(files) == 0 {
		fmt.Println("no file found")
		return nil
	}
	intermediate := []KeyValue{}
	for _, file := range files {
		fhandle, err := os.Open(file)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(fhandle)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		fhandle.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceIndex)
	ofile, err := os.Create(oname)
	if err != nil {
		return nil
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := theReducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return ofile.Close()
}

func heartBeatReport(workerId int) (int, error) {
	args := HeartBeatArgs{
		WorkerId: workerId,
	}
	reply := HeartBeatReply{}
	ok := call("Coordinator.HeartBeat", &args, &reply)
	if !ok {
		fmt.Printf("call HeartBeat failed!\n")
		return 0, errors.New("call HeartBeat failed")
	}
	return reply.WorkerId, nil
}

func getOneTask() (PullTaskReply, error) {
	args := PullTaskArgs{
		WorkerId: theWorkerId,
	}
	reply := PullTaskReply{}
	ok := call("Coordinator.PullTask", &args, &reply)
	if ok {
		// fmt.Printf("PullTask success %v\n", theWorkerId)
		return reply, nil
	} else {
		fmt.Printf("call PullTask failed!\n")
		return PullTaskReply{}, errors.New("call PullTask failed")
	}
}

func reportTaskFinish(taskType int, taskIndex int) {
	args := TaskFinishArgs{
		WorkerId:  theWorkerId,
		TaskType:  taskType,
		TaskIndex: taskIndex,
	}
	if taskType == 0 {
		return
	}
	reply := TaskFinishReply{}
	ok := call("Coordinator.TaskFinish", &args, &reply)
	if ok {
		fmt.Printf("TaskFinish %d  %s\n", theWorkerId, Task{taskType, taskIndex})
	} else {
		fmt.Printf("call TaskFinish failed!\n")
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
