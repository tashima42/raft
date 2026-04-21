package raft

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
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

var heartbeatTimeout time.Duration = time.Millisecond * 100

// NewRaft creates a new Raft instance and initializes or load values from stable storage.
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

// initLog gets all logs from stable storage and applies them
// to the internal key value state machine. It also updates the
// last applied index and current term.
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

// IsLeader returns true if the server is currently the leader, false otherwise
func (r *Raft) IsLeader() bool {
	return r.State == StateLeader
}

// LeaderAPIAddress returns the API address of the current leader,
// or an error if the leader is not found or there is an issue getting the leader id
func (r *Raft) LeaderAPIAddress() (string, error) {
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
	return leader.APIAddress(), nil
}

// AddToLog adds a new log entry to the log and sends append entries requests to peers
// to replicate the log entry
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

// Run starts the main loop and goroutines for the server.
// It calles the candidate state function and leader state function
// based on the current state.
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

// leaderState runs the main loop for the leader state, sending heartbeats to peers at regular intervals
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

// sendHeartBeats sends empty append entries commands to all peers to maintain leadership
// and prevent followers from becoming candidates and starting new elections
func (r *Raft) sendHeartBeats() error {
	return r.sendAppendEntries([]database.LogEntry{}, false)
}

// sendAppendEntries sends append entries commands to all peers and, if enabled, checks if
// a quorum was achieved to consider the commands successfully replicated to the majority
// of the cluster. If a quorum is not achieved, the log entries are removed and an error is returned
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
