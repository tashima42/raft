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

func (s RaftState) String() string {
	switch s {
	case StateLeader:
		return "leader"
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	}
	return ""
}

type Raft struct {
	id           int
	peers        []Peer
	db           database.Database
	Client       Client
	mu           *sync.Mutex
	electionTick <-chan time.Time
	KeyVal       keyVal
	State        RaftState
	ctx          context.Context
}

type Peer struct {
	ID      int
	Address string
}

var (
	minimumElectionTimeoutMS int32 = 300
	maximumElectionTimeoutMS int32 = 2 * minimumElectionTimeoutMS
	// heartbeatMS              int32 = 100
)

func NewRaft(ctx context.Context, db database.Database, client Client, id int, peers []Peer) (*Raft, error) {
	raft := &Raft{
		ctx:          ctx,
		id:           id,
		peers:        peers,
		db:           db,
		Client:       client,
		mu:           &sync.Mutex{},
		electionTick: nil,
		KeyVal:       newKeyVal(),
		State:        StateFollower,
	}

	raft.resetElectionTimeout()

	if err := raft.setCurrentTerm(0); err != nil {
		return nil, err
	}
	if err := raft.setVotedFor(-1); err != nil {
		return nil, err
	}

	if err := raft.initLog(); err != nil {
		return nil, errors.New("failed to init and apply log: " + err.Error())
	}

	return raft, nil
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
		if r.State == StateCandidate {
			r.candidateState()
		}
	}
}

func (r *Raft) candidateState() {
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
	r.mu.Unlock()

	slog.InfoContext(r.ctx, "voting for itself")
	if err := r.setVotedFor(r.id); err != nil {
		log.Fatal("failed to vote for self: " + err.Error())
	}

	slog.InfoContext(r.ctx, "reseting election timeout")
	r.resetElectionTimeout()
	slog.InfoContext(r.ctx, "requesting votes")
	r.requestVotes()

	slog.InfoContext(r.ctx, "unlocking mutex")
}

func (r *Raft) RequestVote(req RequestVoteRequest) (int, bool, error) {
	slog.InfoContext(r.ctx, "locking mutex")
	r.mu.Lock()
	defer r.mu.Unlock()

	slog.InfoContext(r.ctx, "getting current term")
	currentTerm, err := r.currentTerm()
	if err != nil {
		return -1, false, err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	if req.Term < currentTerm {
		slog.InfoContext(r.ctx, fmt.Sprintf("req term smaller than current term: %d < %d", req.Term, currentTerm))
		return currentTerm, false, nil
	}
	votedFor, err := r.votedFor()
	if err != nil {
		return -1, false, err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("voted for: %d", votedFor))

	// TODO: missing log validation
	// no peer ID can be negative
	if votedFor < 0 {
		slog.InfoContext(r.ctx, "didn't vote for anyone")
		err := r.setVotedFor(req.CandidateID)
		return currentTerm, true, err
	}
	slog.InfoContext(r.ctx, "voting false")
	return currentTerm, false, nil
}

func (r *Raft) requestVotes() {
	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, "requesting vote from: "+peer.Address)
	}
}

func (r *Raft) resetElectionTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	slog.InfoContext(r.ctx, "resetting election timeout")
	randTimeout := rand.IntN(int(maximumElectionTimeoutMS - minimumElectionTimeoutMS))
	timeout := int(minimumElectionTimeoutMS) + randTimeout
	r.setElectionTimeout(time.Duration(timeout))
}

func (r *Raft) electionTimer() {
	slog.InfoContext(r.ctx, "running election timer")

	for range r.electionTick {
		slog.InfoContext(r.ctx, "election tick received, locking mutex")
		r.mu.Lock()
		slog.InfoContext(r.ctx, "setting state to candidate and unlocking mutex")
		r.State = StateCandidate
		r.mu.Unlock()
	}
}

func (r *Raft) setElectionTimeout(timeout time.Duration) {
	slog.InfoContext(r.ctx, fmt.Sprintf("setting election timeout to: %d", timeout))
	r.electionTick = time.NewTimer(timeout * time.Millisecond).C
}
