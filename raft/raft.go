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
	id            int
	peers         []*Peer
	db            database.Database
	Client        Client
	mu            *sync.Mutex
	electionTick  <-chan time.Time
	heartBeatTick <-chan time.Time
	KeyVal        keyVal
	State         RaftState
	ctx           context.Context
}

var (
	minimumElectionTimeoutMS int32 = 3000
	maximumElectionTimeoutMS int32 = 2 * minimumElectionTimeoutMS
	heartbeatMS              int32 = 100
)

func NewRaft(ctx context.Context, db database.Database, client Client, id int, peers []*Peer) (*Raft, error) {
	slog.InfoContext(ctx, "creating a new raft instance")
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

	slog.InfoContext(ctx, "resetting election timeout for the first time")
	raft.resetElectionTimeout()
	slog.InfoContext(ctx, "setting heartbeat timeout")
	raft.setHeartBeatTimeout(time.Duration(heartbeatMS))

	slog.InfoContext(ctx, "setting current term to 0")
	if err := raft.setCurrentTerm(0); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "setting voted for to invalid value -1")
	if err := raft.setVotedFor(-1); err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "initiating log on start")
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
	slog.InfoContext(r.ctx, "ranging through logs")
	for _, log := range logs {
		slog.InfoContext(r.ctx, fmt.Sprintf("setting term to: %d", log.Term))
		if err := r.setCurrentTerm(log.Term); err != nil {
			return errors.New("failed to set current term: " + err.Error())
		}

		slog.InfoContext(r.ctx, fmt.Sprintf("executing log on keyvalue state machine: (%s) | %s -> %s", log.Action, log.Key, log.Value))
		if err := r.KeyVal.Exec(KeyValAction(log.Action), log.Key, log.Value); err != nil {
			return errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	return nil
}

// AppendEntries receives entries from the leader and checks if they are valid
// and returns a bool for success or error, the current term and an error
func (r *Raft) AppendEntries(req AppendEntriesRequest) (bool, int, error) {
	// reset election timeout and prevent server from starting new elections
	slog.InfoContext(r.ctx, fmt.Sprintf("received append entries request: %+v", req))
	slog.InfoContext(r.ctx, "reseting election timeout")
	r.resetElectionTimeout()
	r.mu.Lock()
	defer r.mu.Unlock()

	slog.InfoContext(r.ctx, "getting current term")
	currentTerm, err := r.currentTerm()
	if err != nil {
		return false, currentTerm, errors.New("failed to get current term: " + err.Error())
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	// (§5.1)
	if req.Term < currentTerm {
		slog.InfoContext(r.ctx, "request term is smaller than current term, replying false")
		return false, currentTerm, nil
	}

	slog.InfoContext(r.ctx, fmt.Sprintf("setting current term to %d", req.Term))
	if err := r.setCurrentTerm(req.Term); err != nil {
		return false, currentTerm, errors.New("failed to set current term: " + err.Error())
	}
	currentTerm = req.Term
	slog.InfoContext(r.ctx, "appending entries to log")
	if err := r.appendLogs(req.Entries); err != nil {
		return false, currentTerm, errors.New("failed to append logs: " + err.Error())
	}
	// (§5.3)
	slog.InfoContext(r.ctx, "getting count log")
	logCount, err := r.logCount()
	if err != nil {
		return false, currentTerm, errors.New("failed to count logs: " + err.Error())
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("log count is: %d", logCount))
	if req.PrevLogIndex > logCount {
		slog.InfoContext(r.ctx, "previous log index from request is bigger than current log count, replying false")
		return false, currentTerm, nil
	}
	// 	prevLog := r.stableState.Log[req.PrevLogIndex]
	// 	if prevLog.Term != req.Term {
	// 		return false, currentTerm, nil
	// 	}
	//
	slog.InfoContext(r.ctx, "ranging through request entries")
	for _, entry := range req.Entries {
		slog.InfoContext(r.ctx, fmt.Sprintf("executing log on keyvalue state machine: (%s) | %s -> %s", entry.Action, entry.Key, entry.Value))
		if err := r.KeyVal.Exec(KeyValAction(entry.Action), entry.Key, entry.Value); err != nil {
			return false, currentTerm, errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}

	slog.InfoContext(r.ctx, "replying true")
	return true, currentTerm, nil
}

func (r *Raft) Run() {
	slog.InfoContext(r.ctx, "running raft")
	slog.InfoContext(r.ctx, "starting election timer")
	go r.electionTimer()
	for {
		switch r.State {
		case StateFollower:
			// slog.InfoContext(r.ctx, "follower state")
		case StateCandidate:
			slog.InfoContext(r.ctx, "candidate state")
			if err := r.candidateState(); err != nil {
				log.Fatal(err.Error())
			}
		case StateLeader:
			slog.InfoContext(r.ctx, "leader state")
			if err := r.leaderState(); err != nil {
				log.Fatal(err.Error())
			}
		}
	}
}

func (r *Raft) leaderState() error {
	for range r.heartBeatTick {
		slog.InfoContext(r.ctx, "heartbeat tick received")
		if r.State != StateLeader {
			slog.InfoContext(r.ctx, "not in leader state, returning")
			return nil
		}
		slog.InfoContext(r.ctx, "sending heartbeats to peers")
		if err := r.sendHeartBeats(); err != nil {
			return err
		}
	}

	return nil
}

func (r *Raft) candidateState() error {
	slog.InfoContext(r.ctx, "candidate state identified")
	slog.InfoContext(r.ctx, "locking mutex")
	r.mu.Lock()
	currentTerm, err := r.currentTerm()
	if err != nil {
		return errors.New("failed to get current term: " + err.Error())
	}
	currentTerm += 1
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	if err := r.setCurrentTerm(currentTerm); err != nil {
		return errors.New("failed to set current term: " + err.Error())
	}
	r.mu.Unlock()

	slog.InfoContext(r.ctx, "voting for itself")
	if err := r.setVotedFor(r.id); err != nil {
		return errors.New("failed to vote for self: " + err.Error())
	}

	slog.InfoContext(r.ctx, "reseting election timeout")
	r.resetElectionTimeout()
	slog.InfoContext(r.ctx, "requesting votes")
	if err := r.requestVotes(); err != nil {
		return err
	}
	return nil
}

func (r *Raft) sendHeartBeats() error {
	// TODO: only send heartbeats when there are no other append entries requests

	slog.InfoContext(r.ctx, "locking mutex before sending heartbeats to peers")
	r.mu.Lock()
	defer r.mu.Unlock()

	currentTerm, err := r.currentTerm()
	if err != nil {
		return errors.New("failed to get current term: " + err.Error())
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	for _, peer := range r.peers {
		// TODO: implement log index
		slog.InfoContext(r.ctx, fmt.Sprintf("sending append entries request to peer: %d", peer.ID()))
		success, term, err := r.Client.AppendEntries(*peer, AppendEntriesRequest{Term: currentTerm, LeaderID: r.id, PrevLogIndex: -1, PrevLogTerm: -1, Entries: []database.LogEntry{}, LeaderCommit: -1})
		if err != nil {
			return err
		}
		slog.InfoContext(r.ctx, fmt.Sprintf("peer %d responded with success: %t and term: %d", peer.ID(), success, term))
		if !success {
			slog.InfoContext(r.ctx, fmt.Sprintf("peer %d returned false for success", peer.ID()))
			// TODO: implement retries
		}
		if term > currentTerm {
			slog.InfoContext(r.ctx, fmt.Sprintf("peer %d term is bigger than current term, turning into follower", peer.ID()))
			r.State = StateFollower
			return nil
		}
	}

	return nil
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

func (r *Raft) requestVotes() error {
	slog.InfoContext(r.ctx, "requesting votes, locking mutex")
	r.mu.Lock()
	defer r.mu.Unlock()
	currentTerm, err := r.currentTerm()
	if err != nil {
		return err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, "requesting vote from: "+peer.Address())
		// TODO: implement last log
		peerTerm, voteGranted, err := r.Client.RequestVote(*peer, RequestVoteRequest{Term: currentTerm, CandidateID: r.id, LastLogIndex: 0, LastLogTerm: 0})
		if err != nil {
			return err
		}
		slog.InfoContext(r.ctx, fmt.Sprintf("requestd vote response peerID: %d, peerTerm: %d, voteGranted: %t", peer.ID(), peerTerm, voteGranted))
		if peerTerm > currentTerm {
			slog.InfoContext(r.ctx, "peer term is bigger than current term, turning into follower")
			// TODO: set leader
			r.State = StateFollower
			return nil
		}
		if voteGranted {
			slog.InfoContext(r.ctx, "vote granted, setting peer voted for")
			peer.SetVotedFor(r.id)
		}
	}

	slog.InfoContext(r.ctx, "counting votes")
	wonElection, err := r.countVotes()
	if err != nil {
		return err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("voting results: %t", wonElection))
	if wonElection {
		slog.InfoContext(r.ctx, "won election, setting state to leader")
		r.State = StateLeader
	}

	return nil
}

// countVotes checks each peer for votes and if the candidate has a majority, returns true or an error
func (r *Raft) countVotes() (bool, error) {
	slog.InfoContext(r.ctx, "counting votes, setting initial votes to 1")
	totalVotes := 1
	votedFor, err := r.votedFor()
	if err != nil {
		return false, err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("found voted for: %d", votedFor))
	// convert to follower
	if votedFor != r.id {
		slog.InfoContext(r.ctx, "server did not vote for self, setting to -1 and converting to follower state")
		r.State = StateFollower
		if err := r.setVotedFor(-1); err != nil {
			return false, err
		}
		return false, nil
	}
	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, fmt.Sprintf("peer %d voted for: %d", peer.ID(), peer.VotedFor()))
		if peer.VotedFor() == r.id {
			slog.InfoContext(r.ctx, "voted for server, incrementing total votes")
			totalVotes++
		}
	}
	majority := 1 + ((len(r.peers) + 1) / 2)
	slog.InfoContext(r.ctx, fmt.Sprintf("majority of votes needed is: %d", majority))

	if totalVotes < majority {
		slog.InfoContext(r.ctx, fmt.Sprintf("majority of votes is smaller than current total votes: totalVotes %d < majority %d", totalVotes, majority))
		return false, nil
	}
	slog.InfoContext(r.ctx, "majority of votes achieved, returning true for elected")
	return true, nil
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

func (r *Raft) setHeartBeatTimeout(timeout time.Duration) {
	slog.InfoContext(r.ctx, fmt.Sprintf("setting heartbeat timeout to: %d", timeout))
	r.heartBeatTick = time.NewTimer(timeout * time.Millisecond).C
}
