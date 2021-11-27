package mr

//
// RPC definitions.
//
// remember to capitalize all names(by capital,to keep public).
//

import "os"
import "strconv"

type Args struct {
	WorkerState string
}

type Reply struct {
	TaskID int
	MapFileName string
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
