package raft

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/tashima42/raft/database"
)

var ErrQuorumNotReached = errors.New("quorum not reached")

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
	id                 int
	peers              []*Peer
	db                 database.Database
	Client             Client
	mu                 *sync.Mutex
	electionTimeout    time.Duration
	heartbeatTimeout   time.Duration
	electionResetTime  time.Time
	heartbeatResetTime time.Time
	KeyVal             keyVal
	lastApplied        int
	State              RaftState
	ctx                context.Context
}

var (
	minimumElectionTimeoutMS int64         = 300
	maximumElectionTimeoutMS int64         = 2 * minimumElectionTimeoutMS
	heartbeatTimeout         time.Duration = time.Millisecond * 100
)

func NewRaft(ctx context.Context, db database.Database, client Client, id int, peers []*Peer) (*Raft, error) {
	slog.InfoContext(ctx, "creating a new raft instance")
	raft := &Raft{
		ctx:                ctx,
		id:                 id,
		peers:              peers,
		db:                 db,
		Client:             client,
		mu:                 &sync.Mutex{},
		heartbeatTimeout:   heartbeatTimeout,
		electionResetTime:  time.Now(),
		heartbeatResetTime: time.Now(),
		lastApplied:        -1,
		KeyVal:             newKeyVal(),
		State:              StateFollower,
	}

	raft.resetElectionTimeout()

	slog.InfoContext(ctx, "checking if there is a current term")
	if _, err := raft.currentTerm(); err != nil {
		if err != sql.ErrNoRows {
			return nil, errors.New("failed to get current term: " + err.Error())
		}
		slog.InfoContext(ctx, "setting current term to 0")
		if err := raft.setCurrentTerm(0); err != nil {
			return nil, err
		}
	}

	slog.InfoContext(ctx, "checking if there is voted for")
	if _, err := raft.votedFor(); err != nil {
		if err != sql.ErrNoRows {
			return nil, errors.New("failed to get voted for: " + err.Error())
		}
		slog.InfoContext(ctx, "setting voted for to invalid value -1")
		if err := raft.setVotedFor(-1); err != nil {
			return nil, err
		}
	}

	slog.InfoContext(ctx, "checking if there is a prevLogIndex")
	if _, err := raft.prevLogIndex(); err != nil {
		// if there is no prev log index, set to 0 to indicate that there are no logs
		// and prevent the no rows error from being returned when trying to get the prev log index later on
		if err != sql.ErrNoRows {
			return nil, errors.New("failed to get previous log index: " + err.Error())
		}
		if err := raft.setPrevLogIndex(0); err != nil {
			return nil, errors.New("failed to set previous log index: " + err.Error())
		}
	}

	slog.InfoContext(ctx, "checking if there is a prevLogTerm")
	if _, err := raft.prevLogTerm(); err != nil {
		// if there is no prev log term, set to 0 to indicate that there are no logs
		// and prevent the no rows error from being returned when trying to get the prev log term later on
		if err != sql.ErrNoRows {
			return nil, errors.New("failed to get previous log term: " + err.Error())
		}
		if err := raft.setPrevLogTerm(0); err != nil {
			return nil, errors.New("failed to set previous log term: " + err.Error())
		}
	}

	slog.InfoContext(ctx, "checking if there is a leaderID")
	if _, err := raft.leaderID(); err != nil {
		if err != sql.ErrNoRows {
			return nil, errors.New("failed to get leader id: " + err.Error())
		}
		if err := raft.setLeaderID(-1); err != nil {
			return nil, errors.New("failed to set leader id: " + err.Error())
		}
	}

	slog.InfoContext(ctx, "initiating log on start")
	if err := raft.initLog(); err != nil {
		return nil, errors.New("failed to init and apply log: " + err.Error())
	}

	return raft, nil
}

func (r *Raft) GracefullyShutDown() error {
	slog.InfoContext(r.ctx, "gracefully shutting down and closing database")
	if err := r.db.Close(); err != nil {
		return err
	}
	return r.Client.Close()
}

func (r *Raft) initLog() error {
	slog.InfoContext(r.ctx, "initiating log")
	r.mu.Lock()
	defer r.mu.Unlock()
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
		r.lastApplied = log.Index
	}
	return nil
}

func (r *Raft) IsLeader() bool {
	return r.State == StateLeader
}

func (r *Raft) LeaderAddress() (string, error) {
	leaderID, err := r.leaderID()
	if err != nil {
		return "", errors.New("failed to get leader id: " + err.Error())
	}
	var leader *Peer
	for _, peer := range r.peers {
		if peer.ID() == leaderID {
			leader = peer
		}
	}
	if leader == nil {
		return "", errors.New("leader not found")
	}
	return leader.Address(), nil
}

func (r *Raft) AddToLog(action KeyValAction, key, value string) error {
	prevLogIndex, err := r.prevLogIndex()
	if err != nil {
		return errors.New("failed to get previous log index: " + err.Error())
	}
	term, err := r.currentTerm()
	if err != nil {
		return errors.New("failed to get current term: " + err.Error())
	}
	entry := database.LogEntry{Index: prevLogIndex + 1, Term: term, Action: string(action), Key: key, Value: value}
	if err := r.appendLogs([]database.LogEntry{entry}); err != nil {
		return errors.New("failed to append log: " + err.Error())
	}
	if err := r.sendAppendEntries([]database.LogEntry{entry}, true); err != nil {
		if errors.Is(err, ErrQuorumNotReached) {
			// entry
			slog.InfoContext(r.ctx, "quorum not reached for log entry, removing log entry and returning error")
			if err := r.deleteLogsFromIndex(prevLogIndex + 1); err != nil {
				return errors.New("failed to delete log entry after quorum not reached: " + err.Error())
			}
		}
		return errors.New("failed to send append entries: " + err.Error())
	}
	if err := r.setPrevLogIndex(prevLogIndex + 1); err != nil {
		return errors.New("failed to set previous log index: " + err.Error())
	}
	if err := r.setLeaderCommit(prevLogIndex + 1); err != nil {
		return errors.New("failed to set leader commit: " + err.Error())
	}
	return r.KeyVal.Exec(KeyValAction(action), key, value)
}

// AppendEntries receives entries from the leader and checks if they are valid
// and returns a bool for success or error, the current term and an error
func (r *Raft) AppendEntries(req AppendEntriesRequest) (bool, int, error) {
	// reset election timeout and prevent server from starting new elections
	slog.InfoContext(r.ctx, fmt.Sprintf("received append entries request: %+v", req))
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

	// if req.PrevLogIndex
	prevLogTerm, err := r.prevLogTerm()
	if err != nil {
		return false, currentTerm, errors.New("failed to get previous log term: " + err.Error())
	}
	if prevLogTerm != req.PrevLogTerm {
		return false, currentTerm, nil
	}

	// TODO: if there is a conflict with the existing log, delete the existing log and all that follow it

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

	if err := r.appendLogs(req.Entries); err != nil {
		return false, currentTerm, errors.New("failed to append logs: " + err.Error())
	}

	slog.InfoContext(r.ctx, "ranging through request entries")
	for _, entry := range req.Entries {
		slog.InfoContext(r.ctx, fmt.Sprintf("executing log on keyvalue state machine: (%s) | %s -> %s", entry.Action, entry.Key, entry.Value))
		if err := r.KeyVal.Exec(KeyValAction(entry.Action), entry.Key, entry.Value); err != nil {
			return false, currentTerm, errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}

	if err := r.setLeaderID(req.LeaderID); err != nil {
		return false, currentTerm, errors.New("failed to set leader id: " + err.Error())
	}

	slog.InfoContext(r.ctx, "replying true")
	return true, currentTerm, nil
}

func (r *Raft) Run() {
	slog.InfoContext(r.ctx, "running raft")
	slog.InfoContext(r.ctx, "starting election timer")
	go r.electionTimer()
	runTicker := time.NewTicker(10 * time.Millisecond).C
	for range runTicker {
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
	heartbeatTicker := time.NewTicker(10 * time.Millisecond).C
	for range heartbeatTicker {
		slog.InfoContext(r.ctx, "heartbeat tick received")
		if r.State != StateLeader {
			slog.InfoContext(r.ctx, "not in leader state, ignoring heartbeat tick")
			return nil
		}
		slog.InfoContext(r.ctx, "sending heartbeats to peers")

		if time.Since(r.heartbeatResetTime) >= r.heartbeatTimeout {
			if err := r.sendHeartBeats(); err != nil {
				return err
			}
			return nil
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

	r.resetElectionTimeout()
	slog.InfoContext(r.ctx, "requesting votes")
	if err := r.requestVotes(); err != nil {
		return err
	}
	return nil
}

func (r *Raft) sendHeartBeats() error {
	return r.sendAppendEntries([]database.LogEntry{}, false)
}

func (r *Raft) sendAppendEntries(entries []database.LogEntry, checkQuorum bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != StateLeader {
		slog.InfoContext(r.ctx, "not in leader state, cannot send append entries")
		return errors.New("cannot send append entries when not in leader state")
	}

	r.heartbeatResetTime = time.Now()

	currentTerm, err := r.currentTerm()
	if err != nil {
		return errors.New("failed to get current term: " + err.Error())
	}
	prevLogIndex, err := r.prevLogIndex()
	if err != nil {
		return errors.New("failed to get previous log index: " + err.Error())
	}
	prevLogTerm, err := r.prevLogTerm()
	if err != nil {
		return errors.New("failed to get previous log term: " + err.Error())
	}
	leaderCommit, err := r.prevLogTerm()
	if err != nil {
		return errors.New("failed to get leader commit: " + err.Error())
	}

	quorum := 1 + ((len(r.peers) + 1) / 2)
	totalSuccess := 1

	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, fmt.Sprintf("sending append entries request to peer: %d: %s", peer.ID(), peer.Address()))
		appendEntriesReq := AppendEntriesRequest{
			Term:         currentTerm,
			LeaderID:     r.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		slog.InfoContext(r.ctx, fmt.Sprintf("append entries request: %+v", appendEntriesReq))
		success, term, err := r.Client.AppendEntries(*peer, appendEntriesReq)
		if err != nil {
			return errors.New("failed to send append entries request to peer: " + err.Error())
		}
		slog.InfoContext(r.ctx, fmt.Sprintf("peer %d responded with success: %t and term: %d", peer.ID(), success, term))
		if !success {
			slog.InfoContext(r.ctx, fmt.Sprintf("peer %d returned false for success", peer.ID()))
			// TODO: implement retries
		}
		totalSuccess += 1
		if term > currentTerm {
			slog.InfoContext(r.ctx, fmt.Sprintf("peer %d term is bigger than current term, turning into follower", peer.ID()))
			r.State = StateFollower
			return nil
		}
	}
	if checkQuorum && totalSuccess < quorum {
		return ErrQuorumNotReached
	}
	return nil
}

func (r *Raft) RequestVote(req RequestVoteRequest) (int, bool, error) {
	slog.InfoContext(r.ctx, fmt.Sprintf("received request vote request: %+v", req))
	r.resetElectionTimeout()
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
	if req.Term > currentTerm {
		currentTerm = req.Term
		if err := r.setCurrentTerm(currentTerm); err != nil {
			return currentTerm, false, errors.New("failed to set current term: " + err.Error())
		}
	}
	votedFor, err := r.votedFor()
	if err != nil {
		return -1, false, errors.New("failed to get voted for: " + err.Error())
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("request voted - voted for: %d", votedFor))

	lastLogIndex, err := r.prevLogIndex()
	if err != nil {
		return currentTerm, false, errors.New("failed to get last log index: " + err.Error())
	}
	lastLogTerm, err := r.prevLogTerm()
	if err != nil {
		return currentTerm, false, errors.New("failed to get last log term: " + err.Error())
	}

	if votedFor < 0 || votedFor == req.CandidateID {
		if req.LastLogIndex < lastLogIndex || (req.LastLogIndex == lastLogIndex && req.LastLogTerm < lastLogTerm) {
			slog.InfoContext(r.ctx, "candidate's log is not as up to date as current server's log, voting false")
			return currentTerm, false, nil
		}

		slog.InfoContext(r.ctx, "didn't vote for anyone or voted for candidate, voting true and setting voted for to candidate id")
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
		slog.InfoContext(r.ctx, fmt.Sprintf("requeste vote response peerID: %d, peerTerm: %d, voteGranted: %t", peer.ID(), peerTerm, voteGranted))
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
	randTimeout := rand.Int64N(maximumElectionTimeoutMS - minimumElectionTimeoutMS)
	r.electionTimeout = time.Millisecond * time.Duration(minimumElectionTimeoutMS+randTimeout)
	slog.InfoContext(r.ctx, fmt.Sprintf("new election timeout is: %s", r.electionTimeout.String()))
	r.electionResetTime = time.Now()
}

func (r *Raft) electionTimer() {
	slog.InfoContext(r.ctx, "running election timer")

	electionTicker := time.NewTicker(10 * time.Millisecond).C

	for range electionTicker {
		r.mu.Lock()
		if r.State == StateLeader {
			slog.InfoContext(r.ctx, "in leader state, ignoring election tick")
			r.mu.Unlock()
			return
		}
		if time.Since(r.electionResetTime) >= r.electionTimeout {
			slog.InfoContext(r.ctx, "setting state to candidate and unlocking mutex")
			r.State = StateCandidate
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}
