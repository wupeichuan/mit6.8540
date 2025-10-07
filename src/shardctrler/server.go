package shardctrler

import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "sort"
import "time"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	duplicateTable map[int64]DuplicateTableTerm
	applyList      map[int]chan ApplyList

	configs []Config // indexed by config num
}

type JoinOp struct {
	Servers map[int][]string
}

type LeaveOp struct {
	GIDs []int
}

type MoveOp struct {
	Shard int
	GID   int
}

type QueryOp struct {
	Num int
}

type Op struct {
	// Your data here.
	OpType      string
	ClientId    int64
	SequenceNum int
	Join        JoinOp
	Leave       LeaveOp
	Move        MoveOp
	Query       QueryOp
}

type DuplicateTableTerm struct {
	SequenceNum int
	Err         Err
	Config      Config
}

type ApplyList struct {
	clientId    int64
	sequenceNum int
	config      Config
	err         Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	raft.Debug(raft.DServer, "S%d Join, ClientId = %d, SequenceNum = %d", sc.me, args.ClientId, args.SequenceNum)
	if duplicateTableTerm, ok := sc.duplicateTable[args.ClientId]; ok {
		if args.SequenceNum < duplicateTableTerm.SequenceNum {
			reply.Err = ErrExpired
			sc.mu.Unlock()
			return
		} else if args.SequenceNum == duplicateTableTerm.SequenceNum {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	op := Op{
		OpType:      "Join",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Join:        JoinOp{Servers: args.Servers},
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	raft.Debug(raft.DServer, "S%d Join is leader, index = %d", sc.me, index)
	_, ok := sc.applyList[index]
	var kvapplyList chan ApplyList
	if !ok {
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
	} else {
		exkvapplyList := sc.applyList[index]
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
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
		raft.Debug(raft.DServer, "S%d Join rewrite index = %d", sc.me, index)
		reply.Err = ErrApplyFail
		return
	} else {
		reply.Err = m.err
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	raft.Debug(raft.DServer, "S%d Leave, ClientId = %d, SequenceNum = %d", sc.me, args.ClientId, args.SequenceNum)
	if duplicateTableTerm, ok := sc.duplicateTable[args.ClientId]; ok {
		if args.SequenceNum < duplicateTableTerm.SequenceNum {
			reply.Err = ErrExpired
			sc.mu.Unlock()
			return
		} else if args.SequenceNum == duplicateTableTerm.SequenceNum {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	op := Op{
		OpType:      "Leave",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Leave:       LeaveOp{GIDs: args.GIDs},
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	raft.Debug(raft.DServer, "S%d Leave is leader, index = %d", sc.me, index)
	_, ok := sc.applyList[index]
	var kvapplyList chan ApplyList
	if !ok {
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
	} else {
		exkvapplyList := sc.applyList[index]
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
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
		raft.Debug(raft.DServer, "S%d Leave rewrite index = %d", sc.me, index)
		reply.Err = ErrApplyFail
		return
	} else {
		reply.Err = m.err
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	raft.Debug(raft.DServer, "S%d Move, ClientId = %d, SequenceNum = %d", sc.me, args.ClientId, args.SequenceNum)
	if duplicateTableTerm, ok := sc.duplicateTable[args.ClientId]; ok {
		if args.SequenceNum < duplicateTableTerm.SequenceNum {
			reply.Err = ErrExpired
			sc.mu.Unlock()
			return
		} else if args.SequenceNum == duplicateTableTerm.SequenceNum {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	op := Op{
		OpType:      "Move",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Move:        MoveOp{Shard: args.Shard, GID: args.GID},
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	raft.Debug(raft.DServer, "S%d Move is leader, index = %d", sc.me, index)
	_, ok := sc.applyList[index]
	var kvapplyList chan ApplyList
	if !ok {
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
	} else {
		exkvapplyList := sc.applyList[index]
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
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
		raft.Debug(raft.DServer, "S%d Move rewrite index = %d", sc.me, index)
		reply.Err = ErrApplyFail
		return
	} else {
		reply.Err = m.err
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	raft.Debug(raft.DServer, "S%d Query, ClientId = %d, SequenceNum = %d, args.Num = %d", sc.me, args.ClientId, args.SequenceNum, args.Num)
	if duplicateTableTerm, ok := sc.duplicateTable[args.ClientId]; ok {
		if args.SequenceNum < duplicateTableTerm.SequenceNum {
			reply.Err = ErrExpired
			sc.mu.Unlock()
			return
		} else if args.SequenceNum == duplicateTableTerm.SequenceNum {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	op := Op{
		OpType:      "Query",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Query:       QueryOp{Num: args.Num},
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	raft.Debug(raft.DServer, "S%d Query is leader, index = %d", sc.me, index)
	_, ok := sc.applyList[index]
	var kvapplyList chan ApplyList
	if !ok {
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
	} else {
		exkvapplyList := sc.applyList[index]
		sc.applyList[index] = make(chan ApplyList, 1)
		kvapplyList = sc.applyList[index]
		sc.mu.Unlock()
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
		raft.Debug(raft.DServer, "S%d Query rewrite index = %d", sc.me, index)
		reply.Err = ErrApplyFail
		return
	} else {
		reply.Err = m.err
		reply.Config = m.config
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) apply() {
	for {
		m, ok := <-sc.applyCh
		if !ok {
			break
		}
		sc.mu.Lock()
		if !m.CommandValid {
			panic("apply")
		}
		command := (m.Command).(Op)
		raft.Debug(raft.DServer, "S%d apply, OpType = %s, ClientId = %d, SequenceNum = %d, m.CommandIndex = %d",
			sc.me, command.OpType, command.ClientId, command.SequenceNum, m.CommandIndex)
		if command.OpType == "Query" {
			duplicateTableTerm, ok := sc.duplicateTable[command.ClientId]
			if ok && command.SequenceNum == duplicateTableTerm.SequenceNum {
				if kvapplyList, ok := sc.applyList[m.CommandIndex]; ok {
					sc.mu.Unlock()
					kvapplyList <- ApplyList{
						clientId:    command.ClientId,
						sequenceNum: command.SequenceNum,
						config:      duplicateTableTerm.Config,
						err:         duplicateTableTerm.Err,
					}
				} else {
					sc.mu.Unlock()
				}
			} else if ok && command.SequenceNum < duplicateTableTerm.SequenceNum {
				if kvapplyList, ok := sc.applyList[m.CommandIndex]; ok {
					sc.mu.Unlock()
					kvapplyList <- ApplyList{
						clientId:    command.ClientId,
						sequenceNum: command.SequenceNum,
						err:         ErrExpired,
					}
				} else {
					sc.mu.Unlock()
				}
			} else {
				var applyList ApplyList
				kvapplyList, ok := sc.applyList[m.CommandIndex]
				if ok {
					config := sc.commandHandle(command)
					applyList = ApplyList{
						clientId:    command.ClientId,
						sequenceNum: command.SequenceNum,
						config:      config,
						err:         OK,
					}
					sc.duplicateTable[command.ClientId] = DuplicateTableTerm{
						SequenceNum: command.SequenceNum,
						Err:         OK,
						Config:      config,
					}
				} else {
					sc.duplicateTable[command.ClientId] = DuplicateTableTerm{
						SequenceNum: command.SequenceNum,
						Err:         OK,
						Config:      sc.commandHandle(command),
					}
				}
				sc.mu.Unlock()
				if ok {
					kvapplyList <- applyList
				}
			}
		} else {
			duplicateTableTerm, ok := sc.duplicateTable[command.ClientId]
			if ok && command.SequenceNum == duplicateTableTerm.SequenceNum {
				if kvapplyList, ok := sc.applyList[m.CommandIndex]; ok {
					sc.mu.Unlock()
					kvapplyList <- ApplyList{
						clientId:    command.ClientId,
						sequenceNum: command.SequenceNum,
						err:         duplicateTableTerm.Err,
					}
				} else {
					sc.mu.Unlock()
				}
			} else if ok && command.SequenceNum < duplicateTableTerm.SequenceNum {
				if kvapplyList, ok := sc.applyList[m.CommandIndex]; ok {
					sc.mu.Unlock()
					kvapplyList <- ApplyList{
						clientId:    command.ClientId,
						sequenceNum: command.SequenceNum,
						err:         ErrExpired,
					}
				} else {
					sc.mu.Unlock()
				}
			} else {
				var applyList ApplyList
				kvapplyList, ok := sc.applyList[m.CommandIndex]
				if ok {
					applyList = ApplyList{
						clientId:    command.ClientId,
						sequenceNum: command.SequenceNum,
						err:         OK,
					}
					sc.duplicateTable[command.ClientId] = DuplicateTableTerm{
						SequenceNum: command.SequenceNum,
						Err:         OK,
					}
				} else {
					sc.duplicateTable[command.ClientId] = DuplicateTableTerm{
						SequenceNum: command.SequenceNum,
						Err:         OK,
					}
				}
				sc.configs = append(sc.configs, sc.commandHandle(command))
				sc.mu.Unlock()
				if ok {
					kvapplyList <- applyList
				}
			}
		}
	}
}

func (sc *ShardCtrler) commandHandle(command Op) Config {
	if command.OpType == "Join" {
		gids := make([]int, 0)
		for gid, _ := range sc.configs[len(sc.configs)-1].Groups {
			if gid != 0 {
				gids = append(gids, gid)
			}
		}
		for gid, _ := range command.Join.Servers {
			gids = append(gids, gid)
		}

		if len(gids) == 0 {
			Shards := [NShards]int{}
			Groups := make(map[int][]string)
			return Config{
				Num:    len(sc.configs),
				Shards: Shards,
				Groups: Groups,
			}
		}

		sort.Ints(gids)
		freeshard := make([]int, 0)
		for shard := 0; shard < NShards; shard++ {
			if sc.configs[len(sc.configs)-1].Shards[shard] == 0 {
				freeshard = append(freeshard, shard)
			}
		}
		gid2shard := make(map[int][]int)
		for shard := 0; shard < NShards; shard++ {
			gid := sc.configs[len(sc.configs)-1].Shards[shard]
			if gid != 0 {
				gid2shard[gid] = append(gid2shard[gid], shard)
			}
		}

		for _, shard := range freeshard {
			minGid := findMin(gids, gid2shard)
			gid2shard[minGid] = append(gid2shard[minGid], shard)
		}

		avg := NShards / len(gids)
		for {
			maxGid := findMax(gids, gid2shard)
			if len(gid2shard[maxGid]) > avg+1 {
				minGid := findMin(gids, gid2shard)
				gid2shard[minGid] = append(gid2shard[minGid], gid2shard[maxGid][0])
				gid2shard[maxGid] = gid2shard[maxGid][1:]
			} else {
				break
			}
		}
		for {
			minGid := findMin(gids, gid2shard)
			if len(gid2shard[minGid]) < avg {
				maxGid := findMax(gids, gid2shard)
				gid2shard[minGid] = append(gid2shard[minGid], gid2shard[maxGid][0])
				gid2shard[maxGid] = gid2shard[maxGid][1:]
			} else {
				break
			}
		}
		Shards := [NShards]int{}
		for gid, shards := range gid2shard {
			for _, shard := range shards {
				Shards[shard] = gid
			}
		}
		Groups := make(map[int][]string)
		for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
			Groups[gid] = make([]string, len(servers))
			copy(Groups[gid], servers)
		}
		for gid, servers := range command.Join.Servers {
			Groups[gid] = make([]string, len(servers))
			copy(Groups[gid], servers)
		}
		return Config{
			Num:    len(sc.configs),
			Shards: Shards,
			Groups: Groups,
		}
	} else if command.OpType == "Leave" {
		gids := make([]int, 0)
		for gid, _ := range sc.configs[len(sc.configs)-1].Groups {
			if gid != 0 && !isInList(gid, command.Leave.GIDs) {
				gids = append(gids, gid)
			}
		}

		if len(gids) == 0 {
			Shards := [NShards]int{}
			Groups := make(map[int][]string)
			return Config{
				Num:    len(sc.configs),
				Shards: Shards,
				Groups: Groups,
			}
		}

		sort.Ints(gids)
		freeshard := make([]int, 0)
		for shard := 0; shard < NShards; shard++ {
			gid := sc.configs[len(sc.configs)-1].Shards[shard]
			if gid == 0 || isInList(gid, command.Leave.GIDs) {
				freeshard = append(freeshard, shard)
			}
		}

		gid2shard := make(map[int][]int)
		for shard := 0; shard < NShards; shard++ {
			gid := sc.configs[len(sc.configs)-1].Shards[shard]
			if gid != 0 && !isInList(gid, command.Leave.GIDs) {
				gid2shard[gid] = append(gid2shard[gid], shard)
			}
		}

		for _, shard := range freeshard {
			minGid := findMin(gids, gid2shard)
			gid2shard[minGid] = append(gid2shard[minGid], shard)
		}

		avg := NShards / len(gids)
		for {
			maxGid := findMax(gids, gid2shard)
			if len(gid2shard[maxGid]) > avg+1 {
				minGid := findMin(gids, gid2shard)
				gid2shard[minGid] = append(gid2shard[minGid], gid2shard[maxGid][0])
				gid2shard[maxGid] = gid2shard[maxGid][1:]
			} else {
				break
			}
		}
		for {
			minGid := findMin(gids, gid2shard)
			if len(gid2shard[minGid]) < avg {
				maxGid := findMax(gids, gid2shard)
				gid2shard[minGid] = append(gid2shard[minGid], gid2shard[maxGid][0])
				gid2shard[maxGid] = gid2shard[maxGid][1:]
			} else {
				break
			}
		}
		Shards := [NShards]int{}
		for gid, shards := range gid2shard {
			for _, shard := range shards {
				Shards[shard] = gid
			}
		}
		Groups := make(map[int][]string)
		for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
			if !isInList(gid, command.Leave.GIDs) {
				Groups[gid] = make([]string, len(servers))
				copy(Groups[gid], servers)
			}
		}

		return Config{
			Num:    len(sc.configs),
			Shards: Shards,
			Groups: Groups,
		}
	} else if command.OpType == "Move" {
		Shards := [NShards]int{}
		for shard, gid := range sc.configs[len(sc.configs)-1].Shards {
			Shards[shard] = gid
		}
		Shards[command.Move.Shard] = command.Move.GID
		Groups := make(map[int][]string)
		for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
			Groups[gid] = make([]string, len(servers))
			copy(Groups[gid], servers)
		}

		return Config{
			Num:    len(sc.configs),
			Shards: Shards,
			Groups: Groups,
		}
	} else {
		if command.Query.Num == -1 || command.Query.Num > len(sc.configs)-1 {
			return sc.configs[len(sc.configs)-1]
		}
		return sc.configs[command.Query.Num]
	}
}

func findMin(gids []int, gid2shard map[int][]int) int {
	minGid := gids[0]
	min := len(gid2shard[minGid])
	for _, gid := range gids {
		if len(gid2shard[gid]) < min {
			min = len(gid2shard[gid])
			minGid = gid
		}
	}
	return minGid
}

func findMax(gids []int, gid2shard map[int][]int) int {
	maxGid := gids[0]
	max := len(gid2shard[maxGid])
	for _, gid := range gids {
		if len(gid2shard[gid]) > max {
			max = len(gid2shard[gid])
			maxGid = gid
		}
	}
	return maxGid
}

func isInList(gid int, gids []int) bool {
	for _, tmpGid := range gids {
		if gid == tmpGid {
			return true
		}
	}
	return false
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.duplicateTable = make(map[int64]DuplicateTableTerm)
	sc.applyList = make(map[int]chan ApplyList)

	go sc.apply()
	return sc
}
