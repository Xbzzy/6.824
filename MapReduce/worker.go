package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)


// Map functions return a slice of KeyValue.

type KeyValue struct {
	Key   string
	Value string
}

// ByKey :for sorting by key.
type ByKey []KeyValue

// for sorting by key.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose to reduce task
// number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker :
// main/mrworker.go calls this function.
//
func Worker(mapFunction func(string, string) []KeyValue,
	reduceFunction func(string, []string) string) {
	go func() {
		args := new(Args)
		args.ApplyMap = true
		for {
			replyMapTask := RPCCall(args)
			if replyMapTask.MapTaskID == -1 {
				return
			}
			doMap(replyMapTask, mapFunction)
		}
	}()
	go func() {
		args := new (Args)
		args.ApplyMap = false
		for {
			replyReduceTask := RPCCall(args)
			if replyReduceTask.ReduceTaskID == -1 {
				return
			}
			doReduce(replyReduceTask,reduceFunction)
		}
	}()
}

func doMap(replyMapTask *Reply,mapFunction func(string, string) []KeyValue) {
	filename := replyMapTask.MapFileName
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapFunction(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	intermediateFileName := "mr-" + strconv.Itoa(replyMapTask.MapTaskID) + "-" + strconv.Itoa(replyMapTask.ReduceTaskID)
	inFile, err1 := os.Create(intermediateFileName)
	if err1 != nil {
		log.Fatalf("cannot create %v", filename)
	}
	writeToJSON(inFile, intermediate)
	args1 := Args{false,"", replyMapTask.MapTaskID, -1,
		intermediateFileName}
	replyMapCompleted := RPCCall(&args1)
	if replyMapCompleted.RPCState == true {
		return
	}
}

func doReduce(replyReduceTask *Reply,reduceFunction func(string, []string) string)  {
	file,_ := os.Open(replyReduceTask.IntermediateFileName)
	intermediate := readFromJSON(file)
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
		output := reduceFunction(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	file.Close()
	args := new (Args)
	args.ApplyMap = false
	replyReduceCompleted := RPCCall(args)
	if replyReduceCompleted.RPCState == true {
		return
	}
}

// RPCCall :make an RPC call to the coordinator.
func RPCCall(args *Args) *Reply {
	reply := Reply{}
	// send the RPC request, wait for the reply.
	if args.ApplyMap == true {
		go call("Coordinator.ApplyMapTask", &args, &reply)
	} else if args.MapCompletedID != -1{
		go call("Coordinator.CorrectMapTaskState", &args, &reply)
	} else if args.ReduceCompletedID != -1 {
		go call("Coordinator.CorrectReduceTaskState", &args, &reply)
	} else {
		go call("Coordinator.ApplyReduceTask", &args, &reply)
	}
	return &reply
	//fmt.Printf("reply.Y %v\n", reply.MapFileName)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcName string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockName := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()
	err = client.Call(rpcName, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func writeToJSON(file *os.File,data []KeyValue) {
	encoder := json.NewEncoder(file)
	for _,kv := range data {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Fatal("encode:", err)
		}
	}
}

func readFromJSON(file *os.File) []KeyValue {
	decoder := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			break
		}
		kva = append(kva,kv)
	}
	return kva
}
