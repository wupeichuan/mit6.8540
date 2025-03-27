package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "errors"
import "time"

type Coordinator struct {
	// Your definitions here.
	m []MapFiles
	r []ReduceFiles
	isMapFinished bool
	isReduceFinished bool
}

type MapFiles struct {
	filename string
	isfetched bool
	iscomplete bool
	timeout *time.Timer
}

type ReduceFiles struct {
	isfetched bool
	iscomplete bool
	timeout *time.Timer
}

var mu sync.Mutex
// Your code here -- RPC handlers for the worker to call.
func (m *MapFiles) callback() {
	mu.Lock()
	if(!m.iscomplete) {
		m.isfetched = false
	}
	mu.Unlock()
}

func (r *ReduceFiles) callback() {
	mu.Lock()
	if(!r.iscomplete) {
		r.isfetched = false
	}
	mu.Unlock()
}

func (c *Coordinator) FetchTask(args *RpcArgs, reply *RpcReply) error {
	if(args.Workertype == "Map") {
		mu.Lock()
		if(args.IsReply) {
			if(c.m[args.Workernum].isfetched) {
				// fmt.Println(fmt.Sprintf("Map isfetched: %d", args.Workernum))
				c.m[args.Workernum].iscomplete = true
				c.m[args.Workernum].timeout.Stop()
			} else {
				// fmt.Println(fmt.Sprintf("Map isunfetched: %d", args.Workernum))
			}
		} else {
			reply.IsMapFinished = true
			if(!c.isMapFinished) {
				for index, m := range c.m {
					if m.isfetched == false {
						c.m[index].isfetched = true
						c.m[index].iscomplete = false
						c.m[index].timeout = time.AfterFunc(10*time.Second, c.m[index].callback)
						reply.Filename = m.filename
						reply.Workernum = index
						reply.N = len(c.r)
						reply.IsMapFinished = false
						break
					} else {
						if m.iscomplete == false {
							reply.IsMapFinished = false
						}
					}
				}
				if(reply.IsMapFinished) {
					c.isMapFinished = true
				}
			}
		}
		mu.Unlock()
		return nil
	} else if(args.Workertype == "Reduce") {
		mu.Lock()
		if(!c.isMapFinished) {
			reply.IsMapFinished = false
		} else {
			reply.IsMapFinished = true
			if(args.IsReply) {
				if(c.r[args.Workernum].isfetched) {
					// fmt.Println(fmt.Sprintf("Reduce isfetched: %d", args.Workernum))
					c.r[args.Workernum].iscomplete = true
					c.r[args.Workernum].timeout.Stop()
				} else {
					// fmt.Println(fmt.Sprintf("Reduce isunfetched: %d", args.Workernum))
				}
			} else {
				reply.IsReduceFinished = true
				if(!c.isReduceFinished) {
					for index, r := range c.r {
						if r.isfetched == false {
							// fmt.Println(fmt.Sprintf("Reduce: %d", index))
							c.r[index].isfetched = true
							c.r[index].iscomplete = false
							c.r[index].timeout = time.AfterFunc(10*time.Second, c.r[index].callback)
							reply.Workernum = index
							reply.N = len(c.m)
							reply.IsReduceFinished = false
							reply.Filename = "flag"
							break
						} else {
							if r.iscomplete == false {
								reply.IsReduceFinished = false
							}
						}
					}
					if(reply.IsReduceFinished) {
						c.isReduceFinished = true
					}
				}	
			}
		}
		mu.Unlock()
		return nil
	} else {
		return errors.New("workertype undefined!")
	}
	
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	isAllFinished := true
	mu.Lock()
	for _, r := range c.r {
		if(!r.iscomplete) {
			isAllFinished = false
			break
		}
	}
	mu.Unlock()
	return isAllFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, filename := range files {
		c.m = append(c.m, MapFiles{filename, false, false, nil})
	}  
	reduceworkernum := 0
	for reduceworkernum < nReduce {
		c.r = append(c.r, ReduceFiles{false, false, nil})
		reduceworkernum++
	}
	c.isMapFinished = false
	c.isReduceFinished = false
	c.server()
	return &c
}
