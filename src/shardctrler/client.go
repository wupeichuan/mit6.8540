package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "6.5840/raft"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId    int64
	sequenceNum int
	leaderHint  int
}

type SendMsg struct {
	server int
	config Config
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
	// Your code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderHint = -1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.sequenceNum++
	args := QueryArgs{
		Num: num,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	if ck.leaderHint != -1 {
		reply := QueryReply{}
		if ok := ck.servers[ck.leaderHint].Call("ShardCtrler.Query", &args, &reply); ok {
			if reply.WrongLeader {
			} else if reply.Err == OK {
				return reply.Config
			} else if reply.Err == ErrExpired {
			} else if reply.Err == ErrApplyFail {
			}
		} else {
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
			go ck.sendQueryHandle(server, args, getCh, replyCh)
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
	return result.config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.sequenceNum++
	args := JoinArgs{
		Servers: servers,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	if ck.leaderHint != -1 {
		reply := JoinReply{}
		if ok := ck.servers[ck.leaderHint].Call("ShardCtrler.Join", &args, &reply); ok {
			if reply.WrongLeader {
			} else if reply.Err == OK {
				return
			} else if reply.Err == ErrExpired {
			} else if reply.Err == ErrApplyFail {
			}
		} else {
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
			go ck.sendJoinHandle(server, args, getCh, replyCh)
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

func (ck *Clerk) Leave(gids []int) {
	ck.sequenceNum++
	args := LeaveArgs{
		GIDs: gids,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	if ck.leaderHint != -1 {
		reply := LeaveReply{}
		if ok := ck.servers[ck.leaderHint].Call("ShardCtrler.Leave", &args, &reply); ok {
			if reply.WrongLeader {
			} else if reply.Err == OK {
				return
			} else if reply.Err == ErrExpired {
			} else if reply.Err == ErrApplyFail {
			}
		} else {
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
			go ck.sendLeaveHandle(server, args, getCh, replyCh)
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

func (ck *Clerk) Move(shard int, gid int) {
	ck.sequenceNum++
	args := MoveArgs{
		Shard: shard,
		GID:   gid,
		ClientId: ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	if ck.leaderHint != -1 {
		reply := MoveReply{}
		if ok := ck.servers[ck.leaderHint].Call("ShardCtrler.Move", &args, &reply); ok {
			if reply.WrongLeader {
			} else if reply.Err == OK {
				return
			} else if reply.Err == ErrExpired {
			} else if reply.Err == ErrApplyFail {
			}
		} else {
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
			go ck.sendMoveHandle(server, args, getCh, replyCh)
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

func (ck *Clerk) sendQueryHandle(server int, args QueryArgs, getCh chan SendMsg, replyCh chan int) {
	reply := QueryReply{}
	if ok := ck.servers[server].Call("ShardCtrler.Query", &args, &reply); ok {
		if reply.WrongLeader {
		} else if reply.Err == OK {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server, config: reply.Config}
			}
		} else if reply.Err == ErrExpired {
		} else if reply.Err == ErrApplyFail {
		}
	} else {
	}
	replyCh <- server
}

func (ck *Clerk) sendJoinHandle(server int, args JoinArgs, getCh chan SendMsg, replyCh chan int) {
	reply := JoinReply{}
	if ok := ck.servers[server].Call("ShardCtrler.Join", &args, &reply); ok {
		if reply.WrongLeader {
		} else if reply.Err == OK {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server}
			}
		} else if reply.Err == ErrExpired {
		} else if reply.Err == ErrApplyFail {
		}
	} else {
	}
	replyCh <- server
}

func (ck *Clerk) sendLeaveHandle(server int, args LeaveArgs, getCh chan SendMsg, replyCh chan int) {
	reply := LeaveReply{}
	if ok := ck.servers[server].Call("ShardCtrler.Leave", &args, &reply); ok {
		if reply.WrongLeader {
		} else if reply.Err == OK {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server}
			}
		} else if reply.Err == ErrExpired {
		} else if reply.Err == ErrApplyFail {
		}
	} else {
	}
	replyCh <- server
}

func (ck *Clerk) sendMoveHandle(server int, args MoveArgs, getCh chan SendMsg, replyCh chan int) {
	reply := MoveReply{}
	if ok := ck.servers[server].Call("ShardCtrler.Move", &args, &reply); ok {
		if reply.WrongLeader {
		} else if reply.Err == OK {
			if len(getCh) == 0 {
				getCh <- SendMsg{server: server}
			}
		} else if reply.Err == ErrExpired {
		} else if reply.Err == ErrApplyFail {
		}
	} else {
	}
	replyCh <- server
}
