package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType      string
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int
}

type DuplicateTableTerm struct {
	SequenceNum int
	Value       string
	Err         Err
}

type ApplyList struct {
	clientId    int64
	sequenceNum int
	value       string
	err         Err
}

type KVServer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	duplicateTable map[int64]DuplicateTableTerm
	stateMachine   map[string]string
	applyList      map[int]chan ApplyList
	lastApplied    int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	raft.Debug(raft.DServer, "S%d Get, Key = %s, ClientId = %d, SequenceNum = %d", kv.me, args.Key, args.ClientId, args.SequenceNum)
	if duplicateTableTerm, ok := kv.duplicateTable[args.ClientId]; ok {
		if args.SequenceNum < duplicateTableTerm.SequenceNum {
			reply.Err = ErrExpired
			kv.mu.Unlock()
			return
		} else if args.SequenceNum == duplicateTableTerm.SequenceNum {
			reply.Err = OK
			reply.Value = duplicateTableTerm.Value
			kv.mu.Unlock()
			return
		}
	}

	op := Op{
		OpType:      "Get",
		Key:         args.Key,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	raft.Debug(raft.DServer, "S%d Get is leader, index = %d", kv.me, index)
	_, ok := kv.applyList[index]
	var kvapplyList chan ApplyList
	if !ok {
		kv.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = kv.applyList[index]
		kv.mu.Unlock()
	} else {
		exkvapplyList := kv.applyList[index]
		kv.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = kv.applyList[index]
		kv.mu.Unlock()
		exkvapplyList<-ApplyList{
			clientId:    args.ClientId,
			sequenceNum: args.SequenceNum,
		}
	}

	var m ApplyList
	select {
	case m = <-kvapplyList:
	case <-time.After(time.Duration(raft.GetLeaderElectionTime()) * time.Millisecond):
		reply.Err = ErrApplyFail
		return
	}

	if m.clientId != args.ClientId || m.sequenceNum != args.SequenceNum {
		reply.Err = ErrApplyFail
		return
	} else {
		reply.Err = m.err
		reply.Value = m.value
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	raft.Debug(raft.DServer, "S%d PutAppend, OpType = %s, Key = %s, Value = %s, ClientId = %d, SequenceNum = %d",
		kv.me, args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum)
	if duplicateTableTerm, ok := kv.duplicateTable[args.ClientId]; ok {
		if args.SequenceNum < duplicateTableTerm.SequenceNum {
			reply.Err = ErrExpired
			kv.mu.Unlock()
			return
		} else if args.SequenceNum == duplicateTableTerm.SequenceNum {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	op := Op{
		OpType:      args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	raft.Debug(raft.DServer, "S%d PutAppend is leader, index = %d", kv.me, index)
	_, ok := kv.applyList[index]
	var kvapplyList chan ApplyList
	if !ok {
		kv.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = kv.applyList[index]
		kv.mu.Unlock()
	} else {
		exkvapplyList := kv.applyList[index]
		kv.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = kv.applyList[index]
		kv.mu.Unlock()
		exkvapplyList<-ApplyList{
			clientId:    args.ClientId,
			sequenceNum: args.SequenceNum,
		}
	}

	var m ApplyList
	select {
	case m = <-kvapplyList:
	case <-time.After(time.Duration(raft.GetLeaderElectionTime()) * time.Millisecond):
		reply.Err = ErrApplyFail
		return
	}
	
	if m.clientId != args.ClientId || m.sequenceNum != args.SequenceNum {
		raft.Debug(raft.DServer, "S%d PutAppend rewrite index = %d", kv.me, index)
		reply.Err = ErrApplyFail
		return
	} else {
		reply.Err = m.err
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for kv.killed() == false {
		if m, ok := <-kv.applyCh; ok {
			kv.mu.Lock()
			if m.SnapshotValid {
				r := bytes.NewBuffer(m.Snapshot)
				d := labgob.NewDecoder(r)
				var lastApplied int
				var duplicateTable map[int64]DuplicateTableTerm
				var stateMachine   map[string]string
				if d.Decode(&lastApplied) != nil ||
					d.Decode(&duplicateTable) != nil ||
					d.Decode(&stateMachine) != nil {
					panic("d.Decode() != nil")
				} else {
					kv.lastApplied = lastApplied
					kv.duplicateTable = duplicateTable
					kv.stateMachine = stateMachine
				}
				kv.mu.Unlock()
				continue
			}
			command, _ := (m.Command).(Op)
			kv.lastApplied = m.CommandIndex
			raft.Debug(raft.DServer, "S%d apply, OpType = %s, Key = %s, Value = %s, ClientId = %d, SequenceNum = %d m.CommandIndex = %d",
				kv.me, command.OpType, command.Key, command.Value, command.ClientId, command.SequenceNum, m.CommandIndex)
			if command.OpType == "Get" {
				duplicateTableTerm, ok := kv.duplicateTable[command.ClientId]
				if ok && command.SequenceNum == duplicateTableTerm.SequenceNum {
					if kvapplyList, ok := kv.applyList[m.CommandIndex]; ok {
						kv.mu.Unlock()
						kvapplyList <- ApplyList{
							clientId:    command.ClientId,
							sequenceNum: command.SequenceNum,
							value:       duplicateTableTerm.Value,
							err:         duplicateTableTerm.Err,
						}
					} else {
						kv.mu.Unlock()
					}
				} else if ok && command.SequenceNum < duplicateTableTerm.SequenceNum {
					if kvapplyList, ok := kv.applyList[m.CommandIndex]; ok {
						kv.mu.Unlock()
						kvapplyList <- ApplyList{
							clientId:    command.ClientId,
							sequenceNum: command.SequenceNum,
							err:         ErrExpired,
						}
					} else {
						kv.mu.Unlock()
					}
				} else {
					var applyList ApplyList
					kvapplyList, ok := kv.applyList[m.CommandIndex]
					if ok {
						if _, ok := kv.stateMachine[command.Key]; ok {
							applyList = ApplyList{
								clientId:    command.ClientId,
								sequenceNum: command.SequenceNum,
								value:       kv.stateMachine[command.Key],
								err:         OK,
							}
							kv.duplicateTable[command.ClientId] = DuplicateTableTerm{
								SequenceNum: command.SequenceNum,
								Value:       kv.stateMachine[command.Key],
								Err:         OK,
							}
						} else {
							applyList = ApplyList{
								clientId:    command.ClientId,
								sequenceNum: command.SequenceNum,
								err:         ErrNoKey,
							}
							kv.duplicateTable[command.ClientId] = DuplicateTableTerm{
								SequenceNum: command.SequenceNum,
								Value:       kv.stateMachine[command.Key],
								Err:         ErrNoKey,
							}
						}
					} else {
						if _, ok := kv.stateMachine[command.Key]; ok {
							kv.duplicateTable[command.ClientId] = DuplicateTableTerm{
								SequenceNum: command.SequenceNum,
								Value:       kv.stateMachine[command.Key],
								Err:         OK,
							}
						} else {
							kv.duplicateTable[command.ClientId] = DuplicateTableTerm{
								SequenceNum: command.SequenceNum,
								Value:       kv.stateMachine[command.Key],
								Err:         ErrNoKey,
							}
						}
					}
					kv.mu.Unlock()
					if ok {
						kvapplyList <- applyList
					}
				}
			} else {
				duplicateTableTerm, ok := kv.duplicateTable[command.ClientId]
				if ok && command.SequenceNum == duplicateTableTerm.SequenceNum {
					if kvapplyList, ok := kv.applyList[m.CommandIndex]; ok {
						kv.mu.Unlock()
						kvapplyList <- ApplyList{
							clientId:    command.ClientId,
							sequenceNum: command.SequenceNum,
							err:         OK,
						}
					} else {
						kv.mu.Unlock()
					}
				} else if ok && command.SequenceNum < duplicateTableTerm.SequenceNum {
					if kvapplyList, ok := kv.applyList[m.CommandIndex]; ok {
						kv.mu.Unlock()
						kvapplyList <- ApplyList{
							clientId:    command.ClientId,
							sequenceNum: command.SequenceNum,
							err:         ErrExpired,
						}
					} else {
						kv.mu.Unlock()
					}
				} else {
					var applyList ApplyList
					kvapplyList, ok := kv.applyList[m.CommandIndex]
					if ok {
						applyList = ApplyList{
							clientId:    command.ClientId,
							sequenceNum: command.SequenceNum,
							err:         OK,
						}
					}
					kv.duplicateTable[command.ClientId] = DuplicateTableTerm{
						SequenceNum: command.SequenceNum,
					}
					if command.OpType == "Put" {
						kv.stateMachine[command.Key] = command.Value
					} else {
						kv.stateMachine[command.Key] = kv.stateMachine[command.Key] + command.Value
					}
					kv.mu.Unlock()
					if ok {
						kvapplyList <- applyList
					}
				}
			}
		}
	}
}

func (kv *KVServer) snapshot() {
	for kv.killed() == false {
		time.Sleep(time.Duration(5) * time.Millisecond)
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.lastApplied)
			e.Encode(kv.duplicateTable)
			e.Encode(kv.stateMachine)
			raftstate := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, raftstate)
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	data := persister.ReadSnapshot()
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var lastApplied int
		var duplicateTable map[int64]DuplicateTableTerm
		var stateMachine   map[string]string
		if d.Decode(&lastApplied) != nil ||
			d.Decode(&duplicateTable) != nil ||
			d.Decode(&stateMachine) != nil {
			panic("d.Decode() != nil")
		} else {
			kv.lastApplied = lastApplied
			kv.duplicateTable = duplicateTable
			kv.stateMachine = stateMachine
		}
	} else {
		kv.lastApplied = 0
		kv.duplicateTable = make(map[int64]DuplicateTableTerm)
		kv.stateMachine = make(map[string]string)
	}

	kv.applyList = make(map[int]chan ApplyList)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	go kv.snapshot()
	return kv
}
