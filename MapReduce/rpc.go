package mr

// capitalize all names(by capital,to keep public).

import (
	"os"
	"strconv"
)

type Args struct {
	ApplyMap bool
	Key string
	MapCompletedID int
	ReduceCompletedID int
	IntermediateFileName string
	FileSize int64
}

type Reply struct {
	RPCState bool
	MapTaskID int
	ReduceTaskID int
	MapFileName string
	IntermediateFileName string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func (c *Coordinator)coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
