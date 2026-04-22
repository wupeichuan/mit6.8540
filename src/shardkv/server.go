package shardkv

import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type KvOP struct {
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int
}

type ContrlOP struct {
}

type Op struct {
	OpType      opType
	Kv          KvOP
	Contrl      ContrlOP
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	duplicateTable map[int64]DuplicateTableTerm
	stateMachine   map[string]string
	applyList      map[int]chan ApplyList
	lastApplied    int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		exkvapplyList <- ApplyList{
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) apply() {
	for kv.killed() == false {
		if m, ok := <-kv.applyCh; ok {
			kv.mu.Lock()
			if m.SnapshotValid {
				kv.initSnapShot(m.Snapshot)
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
			kv.rf.Snapshot(kv.lastApplied, kv.snapshotData())
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.duplicateTable)
	e.Encode(kv.stateMachine)
	return w.Bytes()
}

func (kv *ShardKV) initSnapShot(data []byte) {
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var lastApplied int
		var duplicateTable map[int64]DuplicateTableTerm
		var stateMachine map[string]string
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
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.initSnapShot(persister.ReadSnapshot())
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyList = make(map[int]chan ApplyList)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	if kv.maxraftstate != -1 {
		go kv.snapshot()
	}
	return kv
}
