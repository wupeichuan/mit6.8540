package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "time"
import "os"
import "io/ioutil"
import "sort"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

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
	for {
		args := RpcArgs{"Map", 0, false}
		reply := RpcReply{"", 0, 0, false, false}
		ok := call("Coordinator.FetchTask", &args, &reply)
		if(!ok) {
			// fmt.Println("Coordinator.FetchTask failed!")
			return
		}

		if(reply.IsMapFinished) {
			break
		}
		if(reply.Filename == "") {
			time.Sleep(time.Second)
			continue
		}

		intermediate := make([][]KeyValue, reply.N)
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
			return
		}
		file.Close()
		kva := mapf(reply.Filename, string(content))

		for _, kv := range kva {
			intermediate[ihash(kv.Key) % reply.N] = append(intermediate[ihash(kv.Key) % reply.N], kv)
		}
		n := 0
		for n < reply.N {
			tmp, err := ioutil.TempFile("", "mr-*")
			if(err != nil) {
				log.Fatalf("create tmpfile failed!")
				return
			}
			
			enc := json.NewEncoder(tmp)
			for _, kv := range intermediate[n] {
				err := enc.Encode(&kv)
				if(err != nil) {
					log.Fatalf("store intermediate failed!")
					return
				}
			}
			tmp.Close()
			os.Rename(tmp.Name(), fmt.Sprintf("mr-%d-%d", reply.Workernum, n))
			n++
		}
		
		args = RpcArgs{"Map", reply.Workernum, true}
		reply = RpcReply{"", 0, 0, false, false}
		ok = call("Coordinator.FetchTask", &args, &reply)
		if(!ok) {
			log.Fatalf("Coordinator.FetchTask failed!")
			return
		}
	}
	
	for {
		args := RpcArgs{"Reduce", 0, false}
		reply := RpcReply{"", 0, 0, false, false}
		ok := call("Coordinator.FetchTask", &args, &reply)
		if(!ok) {
			// fmt.Println("Coordinator.FetchTask failed!")
			return
		}
		if(!reply.IsMapFinished) {
			time.Sleep(time.Second)
			continue
		}
		if(reply.Filename == "") {
			time.Sleep(time.Second)
			continue
		}
		if(reply.IsReduceFinished) {
			break
		}
		n := 0
		kva := make([]KeyValue, 0)
		for n < reply.N {
			tmpfile, err := os.Open(fmt.Sprintf("mr-%d-%d", n, reply.Workernum))
			if err != nil {
				fmt.Println("open tmpfile failed!")
				return
			}
			dec := json.NewDecoder(tmpfile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			tmpfile.Close()
			n++
		}
		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-out-%d", reply.Workernum)
		ofile, _ := os.Create(oname)
		outputs := make([]string, 0)
		keys := make([]string, 0)
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			outputs = append(outputs, reducef(kva[i].Key, values))
			keys = append(keys, kva[i].Key)
			i = j
		}

		for index, output := range outputs {
			fmt.Fprintf(ofile, "%v %v\n", keys[index], output)
		}
		ofile.Close()
		args = RpcArgs{"Reduce", reply.Workernum, true}
		reply = RpcReply{"", 0, 0, false, false}
		ok = call("Coordinator.FetchTask", &args, &reply)
		if(!ok) {
			log.Fatalf("Coordinator.FetchTask failed!")
			return
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
