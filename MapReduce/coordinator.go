package MapReduce

//Coordinator is the master in paper MapReduce.
import (
	"log"
	"math/rand"
)
import "net"
import "net/rpc"
import "net/http"

//string state including "idle"/"in-progress"/"completed"
type state string
//intermediate file args
type fileName string
type fileSize uint
type Coordinator struct {
	NowID int
	// the int key of map is taskID,use to named intermediate files,mr-X-Y
	MapTask map[int]fileName
	MapTaskState map[int]state
	ReduceTask map[int]fileName
	ReduceTaskState map[int]state
	//updates to this location and size information when map tasks are completed.
	IntermediateFiles map[string]fileSize
}

// Respond :an RPC handle function
func (c *Coordinator) ApplyMapTask(args *Args, reply *Reply) error {
	nowID := c.NowID
	for taskID,filename := range c.MapTask {
		// choice an as-yet-unStarted map task
		if taskID < 0 {
			delete(c.MapTask, taskID)
			c.MapTask[nowID] = filename
			c.MapTaskState[nowID] = "in-progress"
			reply.MapFileName = string(c.MapTask[nowID])
			c.NowID++
			return nil
		} else {
			continue
		}
	}
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
	return true
}

// MakeCoordinator :
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := new(Coordinator)
	// Init map task
	for _,str := range files {
		random := -rand.Int()
		coordinator.MapTask[random] = fileName(str)
	}
	// use max nReduce to init reduce task 'map'
	for i:=0; i<nReduce; i++ {
		coordinator.ReduceTask[i]=""
	}
	coordinator.server()
	return coordinator
}
