package raft

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/tashima42/raft/database"
)

type RaftState int

const (
	StateLeader = iota
	StateFollower
	StateCandidate
)

type Raft struct {
	db           *database.Database
	mu           *sync.Mutex
	electionTick <-chan time.Time
}

// type raftState struct {
// 	KeyVal      keyVal `json:"store"`
// 	CurrentTerm int    `json:"currentTerm"`
// 	VotedFor    int    `json:"votedFor"`
// }

type AppendEntriesRequest struct {
	Term         int                 `json:"term"`
	LeaderID     int                 `json:"leaderId"`
	PrevLogIndex int                 `json:"prevLogIndex"`
	PrevLogTerm  int                 `json:"prevLogTerm"`
	Entries      []database.LogEntry `json:"entries"`
	LeaderCommit int                 `json:"leaderCommit"`
}

var (
	minimumElectionTimeoutMS int32 = 300
	maximumElectionTimeoutMS int32 = 2 * minimumElectionTimeoutMS
	// heartbeatMS              int32 = 100
)

func NewRaft(db *database.Database) (*Raft, error) {
	raft := &Raft{
		db:           db,
		mu:           &sync.Mutex{},
		electionTick: nil,
	}

	raft.resetElectionTimeout()

	err := raft.setCurrentTerm(0)

	// if err := raft.initLog(); err != nil {
	// 	return nil, errors.New("failed to init and apply log: " + err.Error())
	// }

	return raft, err
}

func (r *Raft) GracefullyShutDown() error {
	return r.db.Close()
}

// func (r *Raft) initLog() error {
// 	for _, record := range r.stableState.Log {
// 		if err := r.setCurrentTerm(record.Term); err != nil {
// 			return err
// 		}
//
// 		if err := r.stableState.KeyVal.Exec(record.Entry.Action, record.Entry.Key, record.Entry.Value); err != nil {
// 			return errors.New("failed to exec operation on keyVal store: " + err.Error())
// 		}
// 	}
// 	return nil
// }

// AppendEntries receives entries from the leader and checks if they are valid
// and returns a bool for success or error, the current term and an error
func (r *Raft) AppendEntries(req AppendEntriesRequest) (bool, int, error) {
	currentTerm, err := r.currentTerm()
	if err != nil {
		return false, currentTerm, err
	}
	// (§5.1)
	if req.Term < currentTerm {
		return false, currentTerm, nil
	}

	if err := r.setCurrentTerm(req.Term); err != nil {
		return false, currentTerm, err
	}
	if err := r.appendLogs(req.Entries); err != nil {
		return false, currentTerm, err
	}
	// (§5.3)
	// if req.PrevLogIndex > len(r.stableState.Log) {
	// 	return false, currentTerm, nil
	// }
	// 	prevLog := r.stableState.Log[req.PrevLogIndex]
	// 	if prevLog.Term != req.Term {
	// 		return false, currentTerm, nil
	// 	}
	//
	// 	currentTerm = req.Term
	// 	logRecords := make([]LogRecord, len(req.Entries))
	// 	for i, entry := range req.Entries {
	// 		logRecords[i] = LogRecord{
	// 			Term:  req.Term,
	// 			Entry: entry,
	// 		}
	// 		if err := r.stableState.KeyVal.Exec(entry.Action, entry.Key, entry.Value); err != nil {
	// 			return false, currentTerm, errors.New("failed to exec operation on keyVal store: " + err.Error())
	// 		}
	// 	}
	// 	r.stableState.Log = append(r.stableState.Log, logRecords...)
	//
	// 	if err := r.saveState(); err != nil {
	// 		return false, r.stableState.CurrentTerm, err
	// 	}
	return true, currentTerm, nil
}

func (r *Raft) Run() {
	for range r.electionTick {
		// convert to candidate
	}
}

func (r *Raft) resetElectionTimeout() {
	randTimeout := rand.IntN(int(maximumElectionTimeoutMS - minimumElectionTimeoutMS))
	timeout := int(minimumElectionTimeoutMS) + randTimeout
	r.electionTick = time.NewTimer(time.Duration(timeout) * time.Millisecond).C
}
