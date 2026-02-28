package raft

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

type RaftState int

const (
	StateLeader = iota
	StateFollower
	StateCandidate
)

type Raft struct {
	state          RaftState
	stableState    *raftState
	storageFile    *os.File
	storageEncoder *json.Encoder
	storageDecoder *json.Decoder
	mu             *sync.Mutex
	electionTick   <-chan time.Time
}

type raftState struct {
	KeyVal      keyVal      `json:"store"`
	CurrentTerm int         `json:"currentTerm"`
	VotedFor    int         `json:"votedFor"`
	Log         []LogRecord `json:"logRecord"`
}

type LogRecord struct {
	Term  int      `json:"term"`
	Entry LogEntry `json:"entry"`
}

type LogEntry struct {
	Action KeyValAction `json:"action"`
	Key    string       `json:"key"`
	Value  string       `json:"value"`
}

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

var (
	minimumElectionTimeoutMS int32 = 300
	maximumElectionTimeoutMS int32 = 2 * minimumElectionTimeoutMS
	heartbeatMS              int32 = 100
)

func NewRaft() (*Raft, error) {
	f, err := os.OpenFile("store.json", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, errors.New("failed to open store file: " + err.Error())
	}

	// encoder := gob.NewEncoder(f)
	// decoder := gob.NewDecoder(f)
	encoder := json.NewEncoder(f)
	decoder := json.NewDecoder(f)

	raft := &Raft{
		stableState: &raftState{
			KeyVal:      newKeyVal(),
			CurrentTerm: 0,
			VotedFor:    0,
			Log:         []LogRecord{},
		},
		storageFile:    f,
		storageEncoder: encoder,
		storageDecoder: decoder,
		mu:             &sync.Mutex{},
		electionTick:   nil,
	}

	if err := raft.restoreState(); err != nil {
		return nil, errors.New("failed to restore state: " + err.Error())
	}

	if err := raft.initLog(); err != nil {
		return nil, errors.New("failed to init and apply log: " + err.Error())
	}

	return raft, nil
}

func (r *Raft) GracefullyShutDown() error {
	if err := r.saveState(); err != nil {
		return errors.New("failed to store state: " + err.Error())
	}
	if err := r.storageFile.Close(); err != nil {
		return errors.New("failed to close store file: " + err.Error())
	}
	return nil
}

func (r *Raft) saveState() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.storageFile.Seek(0, io.SeekStart); err != nil {
		return errors.New("failed to seek the start of the file: " + err.Error())
	}
	if err := r.storageFile.Truncate(0); err != nil {
		return errors.New("failed to truncate file to 0: " + err.Error())
	}

	if err := r.storageEncoder.Encode(r.stableState); err != nil {
		return errors.New("failed to encode state: " + err.Error())
	}

	if err := r.storageFile.Sync(); err != nil {
		return errors.New("failed to sync file: " + err.Error())
	}

	return nil
}

func (r *Raft) restoreState() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.storageDecoder.Decode(r.stableState); err != nil {
		if err != io.EOF {
			return errors.New("failed to decode state: " + err.Error())
		}
	}

	return nil
}

func (r *Raft) initLog() error {
	for _, record := range r.stableState.Log {
		r.stableState.CurrentTerm = record.Term

		if err := r.stableState.KeyVal.Exec(record.Entry.Action, record.Entry.Key, record.Entry.Value); err != nil {
			return errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	return nil
}

// AppendEntries receives entries from the leader and checks if they are valid
// and returns a bool for success or error, the current term and an error
func (r *Raft) AppendEntries(req AppendEntriesRequest) (bool, int, error) {
	// (§5.1)
	if req.Term < r.stableState.CurrentTerm {
		return false, r.stableState.CurrentTerm, nil
	}
	// (§5.3)
	if req.PrevLogIndex > len(r.stableState.Log) {
		return false, r.stableState.CurrentTerm, nil
	}
	prevLog := r.stableState.Log[req.PrevLogIndex]
	if prevLog.Term != req.Term {
		return false, r.stableState.CurrentTerm, nil
	}

	r.stableState.CurrentTerm = req.Term
	logRecords := make([]LogRecord, len(req.Entries))
	for i, entry := range req.Entries {
		logRecords[i] = LogRecord{
			Term:  req.Term,
			Entry: entry,
		}
		if err := r.stableState.KeyVal.Exec(entry.Action, entry.Key, entry.Value); err != nil {
			return false, r.stableState.CurrentTerm, errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	r.stableState.Log = append(r.stableState.Log, logRecords...)

	if err := r.saveState(); err != nil {
		return false, r.stableState.CurrentTerm, err
	}
	return true, r.stableState.CurrentTerm, nil
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
