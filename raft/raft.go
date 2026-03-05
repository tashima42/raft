package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
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
	id           int
	peers        []Peer
	db           *database.Database
	mu           *sync.Mutex
	electionTick <-chan time.Time
	KeyVal       keyVal
	state        RaftState
	ctx          context.Context
}

type Peer struct {
	ID      int
	Address string
}

type AppendEntriesRequest struct {
	Term         int                 `json:"term"`
	LeaderID     int                 `json:"leaderId"`
	PrevLogIndex int                 `json:"prevLogIndex"`
	PrevLogTerm  int                 `json:"prevLogTerm"`
	Entries      []database.LogEntry `json:"entries,omitempty"`
	LeaderCommit int                 `json:"leaderCommit"`
}

var (
	minimumElectionTimeoutMS int32 = 300
	maximumElectionTimeoutMS int32 = 2 * minimumElectionTimeoutMS
	// heartbeatMS              int32 = 100
)

func NewRaft(ctx context.Context, db *database.Database, id int, peers []Peer) (*Raft, error) {
	raft := &Raft{
		ctx:          ctx,
		id:           id,
		peers:        peers,
		db:           db,
		mu:           &sync.Mutex{},
		electionTick: nil,
		KeyVal:       newKeyVal(),
		state:        StateFollower,
	}

	raft.resetElectionTimeout()

	err := raft.setCurrentTerm(0)

	if err := raft.initLog(); err != nil {
		return nil, errors.New("failed to init and apply log: " + err.Error())
	}

	return raft, err
}

func (r *Raft) GracefullyShutDown() error {
	slog.InfoContext(r.ctx, "gracefully shutting down and closing database")
	return r.db.Close()
}

func (r *Raft) initLog() error {
	slog.InfoContext(r.ctx, "initiating log")
	logs, err := r.logs()
	if err != nil {
		return errors.New("failed to get logs: " + err.Error())
	}
	for _, log := range logs {
		if err := r.setCurrentTerm(log.Term); err != nil {
			return err
		}

		if err := r.KeyVal.Exec(KeyValAction(log.Action), log.Key, log.Value); err != nil {
			return errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	return nil
}

// AppendEntries receives entries from the leader and checks if they are valid
// and returns a bool for success or error, the current term and an error
func (r *Raft) AppendEntries(req AppendEntriesRequest) (bool, int, error) {
	currentTerm, err := r.currentTerm()
	if err != nil {
		return false, currentTerm, errors.New("failed to get current term: " + err.Error())
	}
	// (§5.1)
	if req.Term < currentTerm {
		return false, currentTerm, nil
	}

	if err := r.setCurrentTerm(req.Term); err != nil {
		return false, currentTerm, errors.New("failed to set current term: " + err.Error())
	}
	currentTerm = req.Term
	if err := r.appendLogs(req.Entries); err != nil {
		return false, currentTerm, errors.New("failed to append logs: " + err.Error())
	}
	// (§5.3)
	logCount, err := r.logCount()
	if err != nil {
		return false, currentTerm, errors.New("failed to count logs: " + err.Error())
	}
	if req.PrevLogIndex > logCount {
		return false, currentTerm, nil
	}
	// 	prevLog := r.stableState.Log[req.PrevLogIndex]
	// 	if prevLog.Term != req.Term {
	// 		return false, currentTerm, nil
	// 	}
	//
	for _, entry := range req.Entries {
		if err := r.KeyVal.Exec(KeyValAction(entry.Action), entry.Key, entry.Value); err != nil {
			return false, currentTerm, errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	return true, currentTerm, nil
}

func (r *Raft) Run() {
	slog.InfoContext(r.ctx, "running raft")
	go r.electionTimer()
	for {
		if r.state == StateCandidate {
			slog.InfoContext(r.ctx, "candidate state identified")
			slog.InfoContext(r.ctx, "locking mutex")
			r.mu.Lock()
			currentTerm, err := r.currentTerm()
			if err != nil {
				log.Fatal("failed to get current term: " + err.Error())
			}
			currentTerm += 1
			slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
			if err := r.setCurrentTerm(currentTerm); err != nil {
				log.Fatal("failed to set current term: " + err.Error())
			}

			slog.InfoContext(r.ctx, "voting for itself")
			if err := r.setVotedFor(r.id); err != nil {
				log.Fatal("failed to vote for self: " + err.Error())
			}

			slog.InfoContext(r.ctx, "reseting election timeout")
			r.resetElectionTimeout()
			slog.InfoContext(r.ctx, "requesting votes")
			r.requestVotes()

			slog.InfoContext(r.ctx, "unlocking mutex")
			r.mu.Unlock()
		}
	}
}

func (r *Raft) requestVotes() {
	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, "requesting vote from: "+peer.Address)
	}
}

func (r *Raft) resetElectionTimeout() {
	randTimeout := rand.IntN(int(maximumElectionTimeoutMS - minimumElectionTimeoutMS))
	timeout := int(minimumElectionTimeoutMS) + randTimeout
	r.electionTick = time.NewTimer(time.Duration(timeout) * time.Millisecond).C
	slog.InfoContext(r.ctx, fmt.Sprintf("election timeout set to: %d", timeout))
}

func (r *Raft) electionTimer() {
	slog.InfoContext(r.ctx, "running election timer")

	for range r.electionTick {
		slog.InfoContext(r.ctx, "election tick received, locking mutex")
		r.mu.Lock()
		slog.InfoContext(r.ctx, "setting state to candidate and unlocking mutex")
		r.state = StateCandidate
		r.mu.Unlock()
	}
}
