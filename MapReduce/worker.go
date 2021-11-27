package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
func Worker(mapFunction func(string, string) []KeyValue,
	reduceFunction func(string, []string) string) {
	reply := Call()
	filename := reply.MapFileName
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

}

// Call 'function' show how to make an RPC call to the coordinator.
func Call() *Reply {
	args := Args{}
	reply := Reply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.ApplyMapTask", &args, &reply)
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
