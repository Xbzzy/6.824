package mr

//Coordinator is the master in paper MapReduce.
import (
	"errors"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
)

//string state including "idle"/"in-progress"/"completed"
type state string
//intermediate file args
type fileName string
type fileSize int64
type Coordinator struct {
	NowMapID int
	// the int key of map is taskID,use to named intermediate files,mr-X-Y
	MapTask map[int]fileName
	MapTaskState map[int]state
	UncompletedMapTaskNum int
	ReduceTask map[int]fileName
	ReduceTaskState map[int]state
	UncompletedReduceTaskNum int
	//updates to this location and size information when map tasks are completed.
	IntermediateFiles map[string]fileSize
}

// Respond :an RPC handle function
func (c *Coordinator) ApplyMapTask(args *Args, reply *Reply) error {
	nowID := c.NowMapID
	if args.ApplyMap == false {
		return errors.New("apply fail")
	}
	for taskID,filename := range c.MapTask {
		// choice an as-yet-unStarted map task
		if taskID < 0 {
			delete(c.MapTask, taskID)
			c.MapTask[nowID] = filename
			c.MapTaskState[nowID] = "in-progress"
			reply.MapFileName = string(c.MapTask[nowID])
			c.NowMapID++
			return nil
		} else {
			continue
		}
	}
	return nil
}

func (c *Coordinator) ApplyReduceTask(args *Args, reply *Reply) error {
	if c.UncompletedReduceTaskNum == 0 {
		reply.ReduceTaskID = -1
		return nil
	}
	for {
		reduceTaskID := ihash(args.Key) % 10
		if c.ReduceTaskState[reduceTaskID] != "completed" {
			reply.ReduceTaskID = reduceTaskID
			reply.IntermediateFileName = string(c.ReduceTask[reduceTaskID])
			return nil
		} else {
			continue
		}
	}
}

func (c *Coordinator) CorrectMapTaskState(args *Args,reply *Reply) error {
	c.MapTaskState[args.MapCompletedID] = "completed"
	c.IntermediateFiles[args.IntermediateFileName] = fileSize(args.FileSize)
	c.UncompletedMapTaskNum--
	c.UncompletedReduceTaskNum++
	reply.RPCState = true
	return nil
}

func (c *Coordinator) CorrectReduceTaskState(args *Args, reply *Reply) error {
	c.ReduceTaskState[args.ReduceCompletedID] = "completed"
	c.UncompletedReduceTaskNum--
	reply.RPCState = true
	return nil
}

// start a thread(goroutine) that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done :
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _,stateResult:= range c.MapTaskState {
		if stateResult == "completed"{
			continue
		} else {
			return false
		}
	}
	for _,stateResult:= range c.ReduceTaskState {
		if stateResult == "completed"{
			continue
		} else {
			return false
		}
	}
	return true
}

// MakeCoordinator :
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := new(Coordinator)
	mapTaskNum := 0
	// Init map task
	for _,str := range files {
		random := -rand.Int()
		coordinator.MapTask[random] = fileName(str)
		mapTaskNum ++
	}
	coordinator.UncompletedMapTaskNum = mapTaskNum
	// use max nReduce to init reduce task 'map'
	for i:=0; i<nReduce; i++ {
		coordinator.ReduceTask[i]=""
	}
	coordinator.server()
	return coordinator
}
