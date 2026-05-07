// Package raft implements the raft protocol
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
	id                     int
	peers                  []*Peer
	db                     database.Database
	Client                 Client
	mu                     *sync.Mutex
	initializationCooldown time.Duration
	electionTimeout        time.Duration
	heartbeatTimeout       time.Duration
	electionResetTime      time.Time
	heartbeatResetTime     time.Time
	sendLogsChan           chan LogEntry
	receiveLogsChan        chan LogEntry
	lastApplied            int
	State                  RaftState
	ctx                    context.Context
}

type LogEntry struct {
	Entry   []byte
	ErrChan chan error
}

var heartbeatTimeout time.Duration = time.Millisecond * 100

// NewRaft creates a new Raft instance and initializes or load values from stable storage.
func NewRaft(ctx context.Context, db database.Database, client Client, id int, peers []*Peer, initializationCooldownSecs int, receiveLogsChan chan LogEntry) (*Raft, error) {
	slog.InfoContext(ctx, "creating a new raft instance")
	raft := &Raft{
		ctx:                    ctx,
		id:                     id,
		peers:                  peers,
		db:                     db,
		Client:                 client,
		mu:                     &sync.Mutex{},
		initializationCooldown: time.Duration(initializationCooldownSecs) * time.Second,
		heartbeatTimeout:       heartbeatTimeout,
		electionResetTime:      time.Now(),
		heartbeatResetTime:     time.Now(),
		lastApplied:            -1,
		sendLogsChan:           make(chan LogEntry),
		receiveLogsChan:        receiveLogsChan,
		State:                  StateFollower,
	}

	raft.resetElectionTimeout()
	raft.mu.Lock()
	raft.electionTimeout = raft.electionTimeout + raft.initializationCooldown
	raft.mu.Unlock()

	slog.InfoContext(ctx, "checking if there is a current term")
	if _, err := raft.currentTerm(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get current term: %w", err)
		}
		slog.InfoContext(ctx, "setting current term to 0")
		if err := raft.setCurrentTerm(0); err != nil {
			return nil, err
		}
	}

	slog.InfoContext(ctx, "checking if there is voted for")
	if _, err := raft.votedFor(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get voted for: %w", err)
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
			return nil, fmt.Errorf("failed to get previous log index: %w", err)
		}
		if err := raft.setPrevLogIndex(0); err != nil {
			return nil, fmt.Errorf("failed to set previous log index: %w", err)
		}
	}

	slog.InfoContext(ctx, "checking if there is a prevLogTerm")
	if _, err := raft.prevLogTerm(); err != nil {
		// if there is no prev log term, set to 0 to indicate that there are no logs
		// and prevent the no rows error from being returned when trying to get the prev log term later on
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get previous log term: %w", err)
		}
		if err := raft.setPrevLogTerm(0); err != nil {
			return nil, fmt.Errorf("failed to set previous log term: %w", err)
		}
	}

	slog.InfoContext(ctx, "checking if there is a leaderID")
	if _, err := raft.leaderID(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get leader id: %w", err)
		}
		if err := raft.setLeaderID(-1); err != nil {
			return nil, fmt.Errorf("failed to set leader id: %w", err)
		}
	}

	slog.InfoContext(ctx, "initiating log on start")
	if err := raft.initLog(); err != nil {
		return nil, fmt.Errorf("failed to init and apply log: %w", err)
	}

	return raft, nil
}

func (r *Raft) C() <-chan LogEntry {
	return r.sendLogsChan
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
		return fmt.Errorf("failed to get logs: %w", err)
	}
	slog.InfoContext(r.ctx, "ranging through logs")
	for _, log := range logs {
		slog.InfoContext(r.ctx, fmt.Sprintf("setting term to: %d", log.Term))
		if err := r.setCurrentTerm(log.Term); err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}

		if err := r.sendLogToClient(log.Entry); err != nil {
			return fmt.Errorf("failed to send log to client: %w", err)
		}
		r.lastApplied = log.Index
	}
	return nil
}

func (r *Raft) sendLogToClient(entry []byte) error {
	errChan := make(chan error, 1)

	r.sendLogsChan <- LogEntry{
		Entry:   entry,
		ErrChan: errChan,
	}

	err := <-errChan

	return err
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
		return "", fmt.Errorf("failed to get leader id: %w", err)
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
// TODO: Add a goroutine to listen on the receive logs chan and call add to log
func (r *Raft) AddToLog(entry []byte) error {
	prevLogIndex, err := r.prevLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get previous log index: %w", err)
	}
	term, err := r.currentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %w", err)
	}
	dbEntry := database.LogEntry{Index: prevLogIndex + 1, Term: term, Entry: entry}
	if err := r.appendLogs([]database.LogEntry{dbEntry}); err != nil {
		return fmt.Errorf("failed to append log: %w", err)
	}
	if err := r.sendAppendEntries([]database.LogEntry{dbEntry}, true); err != nil {
		if errors.Is(err, ErrQuorumNotReached) {
			// entry
			slog.InfoContext(r.ctx, "quorum not reached for log entry, removing log entry and returning error")
			if err := r.deleteLogsFromIndex(prevLogIndex + 1); err != nil {
				return fmt.Errorf("failed to delete log entry after quorum not reached: %w", err)
			}
		}
		return fmt.Errorf("failed to send append entries: %w", err)
	}
	if err := r.sendLogToClient(entry); err != nil {
		return fmt.Errorf("failed to send log to client: %w", err)
	}

	if err := r.setPrevLogIndex(prevLogIndex + 1); err != nil {
		return fmt.Errorf("failed to set previous log index: %w", err)
	}
	if err := r.setLeaderCommit(prevLogIndex + 1); err != nil {
		return fmt.Errorf("failed to set leader commit: %w", err)
	}
	return nil
}

// Run starts the main loop and goroutines for the server.
// It calles the candidate state function and leader state function
// based on the current state.
func (r *Raft) Run() {
	slog.InfoContext(r.ctx, "running raft")
	slog.InfoContext(r.ctx, "waiting for initilizaition cooldown")

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
			slog.Info("time since last heartbeat bigger than timeout, sending append entries")
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
		return fmt.Errorf("failed to get current term: %w", err)
	}
	prevLogIndex, err := r.prevLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get previous log index: %w", err)
	}
	prevLogTerm, err := r.prevLogTerm()
	if err != nil {
		return fmt.Errorf("failed to get previous log term: %w", err)
	}
	leaderCommit, err := r.prevLogTerm()
	if err != nil {
		return fmt.Errorf("failed to get leader commit: %w", err)
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
		tCtx, cancel := context.WithTimeout(r.ctx, time.Second*1)
		defer cancel()
		success, term, err := r.Client.AppendEntries(tCtx, *peer, appendEntriesReq)
		if err != nil {
			slog.ErrorContext(r.ctx, "failed to send append entries request to peer: "+err.Error())
			success = false
			term = -1
		}
		slog.InfoContext(r.ctx, fmt.Sprintf("peer %d responded with success: %t and term: %d", peer.ID(), success, term))
		if !success {
			slog.InfoContext(r.ctx, fmt.Sprintf("peer %d returned false for success", peer.ID()))
			// TODO: implement retries
			continue
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
