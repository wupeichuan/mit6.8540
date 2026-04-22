package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/raft"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64
	sequenceNum int
	leaderHint  int
}

type SendMsg struct {
	server int
	err    Err
	value  string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderHint = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.sequenceNum++
	raft.Debug(raft.DClient, "C%d Get, key = %s, ck.sequenceNum = %d, ck.leaderHint = %d", ck.clientId, key, ck.sequenceNum, ck.leaderHint)
	args := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	// TODO: If reply error but not ErrWrongLeader, it's like the leader has not changed so we should retry sending RPC to the same server.
	if ck.leaderHint != -1 {
		reply := GetReply{}
		if ok := ck.servers[ck.leaderHint].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else if reply.Err == ErrWrongLeader {
			} else if reply.Err == ErrExpired {
			} else if reply.Err == ErrApplyFail {
			}
		} else {
			raft.Debug(raft.DClient, "C%d Get cannot receive from %d", ck.clientId, ck.leaderHint)
		}
	}

	ck.leaderHint = -1
	getCh := make(chan SendMsg, 1)
	var result SendMsg
	hasresult := false
	for !hasresult {
		replyCh := make(chan int, len(ck.servers))
		isreply := make([]bool, len(ck.servers))
		for server := 0; server < len(ck.servers); server++ {
			go ck.sendGetHandle(server, args, getCh, replyCh)
		}
		for {
			select {
			case result = <-getCh:
				hasresult = true
			case server := <-replyCh:
				isreply[server] = true
			}
			if hasresult {
				break
			}
			isAllReply := true
			for server := 0; server < len(ck.servers); server++ {
				if !isreply[server] {
					isAllReply = false
					break
				}
			}
			if isAllReply {
				break
			}
		}
		if !hasresult {
			time.Sleep(time.Duration(raft.GetLeaderElectionTime()) * time.Millisecond)
		}
	}
	ck.leaderHint = result.server
	if result.err == OK {
		return result.value
	} else {
		return ""
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sequenceNum++
	raft.Debug(raft.DClient, "C%d PutAppend, key = %s, value = %s, op = %s, ck.sequenceNum = %d, ck.leaderHint = %d", ck.clientId, key, value, op, ck.sequenceNum, ck.leaderHint)
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	if ck.leaderHint != -1 {
		reply := PutAppendReply{}
		if ok := ck.servers[ck.leaderHint].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrWrongLeader {
			} else if reply.Err == ErrExpired {
			} else if reply.Err == ErrApplyFail {
			}
		} else {
			raft.Debug(raft.DClient, "C%d PutAppend cannot receive from %d", ck.clientId, ck.leaderHint)
		}
	}

	ck.leaderHint = -1
	getCh := make(chan SendMsg, 1)
	var result SendMsg
	hasresult := false
	for !hasresult {
		replyCh := make(chan int, len(ck.servers))
		isreply := make([]bool, len(ck.servers))
		for server := 0; server < len(ck.servers); server++ {
			go ck.sendPutAppendHandle(server, args, getCh, replyCh)
		}
		for {
			select {
			case result = <-getCh:
				hasresult = true
			case server := <-replyCh:
				isreply[server] = true
			}
			if hasresult {
				break
			}
			isAllReply := true
			for server := 0; server < len(ck.servers); server++ {
				if !isreply[server] {
					isAllReply = false
					break
				}
			}
			if isAllReply {
				break
			}
		}
		if !hasresult {
			time.Sleep(time.Duration(raft.GetLeaderElectionTime()) * time.Millisecond)
		}
	}
	ck.leaderHint = result.server
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGetHandle(server int, args GetArgs, getCh chan SendMsg, replyCh chan int) {
	reply := GetReply{}
	if ok := ck.servers[server].Call("KVServer.Get", &args, &reply); ok {
		if reply.Err == OK {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server, err: reply.Err, value: reply.Value}
			}
		} else if reply.Err == ErrNoKey {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server, err: reply.Err, value: reply.Value}
			}
		} else if reply.Err == ErrWrongLeader {
		} else if reply.Err == ErrExpired {
		} else if reply.Err == ErrApplyFail {
		}
	} else {
	}
	replyCh <- server
}

func (ck *Clerk) sendPutAppendHandle(server int, args PutAppendArgs, getCh chan SendMsg, replyCh chan int) {
	reply := PutAppendReply{}
	if ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply); ok {
		if reply.Err == OK {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server, err: reply.Err}
			}
		} else if reply.Err == ErrWrongLeader {
		} else if reply.Err == ErrExpired {
		} else if reply.Err == ErrApplyFail {
		}
	} else {
	}
	replyCh <- server
}
