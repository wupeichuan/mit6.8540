package raft

import "log"
import "time"
import "fmt"
import "math/rand"

// Debugging
type logTopic string
const (
	DClient  logTopic = "CLNT"
	DServer  logTopic = "SERV"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
func InitDebug() {
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	fmt.Sprintf("InitDebug")
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if topic == DClient || topic == DServer {
		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func Min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MaxTime(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	} else {
		return b
	}
}

func RemoveRep(slc []int) []int {
	result := []int{}
	tempMap := map[int]byte{}
	for _, e := range slc {
		if _, ok := tempMap[e]; !ok {
			tempMap[e] = 0
			result = append(result, e)
		}
	}
	return result
}

func GetRandomTime() int64 {
	return 210+rand.Int63()%250
}

func GetLeaderElectionTime() int64 {
	return 210
}

func GetHeartBeatTime() int64 {
	return 140
}