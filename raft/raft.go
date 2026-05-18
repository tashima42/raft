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
	logger                 *slog.Logger
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
	cancel                 context.CancelFunc
}

type LogEntry struct {
	Entry   []byte
	ErrChan chan error
}

var heartbeatTimeout time.Duration = time.Millisecond * 100

// NewRaft creates a new Raft instance and initializes or load values from stable storage.
func NewRaft(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger, db database.Database, client Client, id int, peers []*Peer, initializationCooldownSecs int, receiveLogsChan chan LogEntry) (*Raft, error) {
	logger = logger.With("node_id", id)
	logger.InfoContext(ctx, "creating a new raft instance")
	raft := &Raft{
		ctx:                    ctx,
		cancel:                 cancel,
		id:                     id,
		peers:                  peers,
		db:                     db,
		logger:                 logger,
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

	raft.logger.InfoContext(ctx, "checking if there is a current term")
	if _, err := raft.currentTerm(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("new - failed to get current term: %w", err)
		}
		raft.logger.InfoContext(ctx, "setting current term to 0")
		if err := raft.setCurrentTerm(0); err != nil {
			return nil, err
		}
	}

	raft.logger.InfoContext(ctx, "checking if there is voted for")
	if _, err := raft.votedFor(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get voted for: %w", err)
		}
		raft.logger.InfoContext(ctx, "setting voted for to invalid value -1")
		if err := raft.setVotedFor(-1); err != nil {
			return nil, err
		}
	}

	raft.logger.InfoContext(ctx, "checking if there is a prevLogIndex")
	if _, err := raft.lastLogIndex(); err != nil {
		// if there is no prev log index, set to 0 to indicate that there are no logs
		// and prevent the no rows error from being returned when trying to get the prev log index later on
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get previous log index: %w", err)
		}
		if err := raft.setLastLogIndex(0); err != nil {
			return nil, fmt.Errorf("failed to set previous log index: %w", err)
		}
	}

	raft.logger.InfoContext(ctx, "checking if there is a lastLogTerm")
	if _, err := raft.lastLogTerm(); err != nil {
		// if there is no prev log term, set to 0 to indicate that there are no logs
		// and prevent the no rows error from being returned when trying to get the prev log term later on
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get previous log term: %w", err)
		}
		if err := raft.setLastLogTerm(0); err != nil {
			return nil, fmt.Errorf("failed to set previous log term: %w", err)
		}
	}

	raft.logger.InfoContext(ctx, "checking if there is a leaderCommit")
	if _, err := raft.leaderCommit(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get leader commit: %w", err)
		}
		if err := raft.setLeaderCommit(0); err != nil {
			return nil, fmt.Errorf("failed to set leader commit: %w", err)
		}
	}

	raft.logger.InfoContext(ctx, "checking if there is a leaderID")
	if _, err := raft.leaderID(); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get leader id: %w", err)
		}
		if err := raft.setLeaderID(-1); err != nil {
			return nil, fmt.Errorf("failed to set leader id: %w", err)
		}
	}

	return raft, nil
}

func (r *Raft) C() <-chan LogEntry {
	return r.sendLogsChan
}

func (r *Raft) GracefullyShutDown() error {
	r.cancel()
	r.logger.InfoContext(r.ctx, "gracefully shutting down and closing database")
	if err := r.Client.Close(); err != nil {
		return err
	}
	return r.db.Close()
}

// initLog gets all logs from stable storage and applies them
// to the internal key value state machine. It also updates the
// last applied index and current term.
func (r *Raft) initLog() error {
	r.logger.InfoContext(r.ctx, "initiating log")
	r.mu.Lock()
	defer r.mu.Unlock()
	logs, err := r.logs()
	if err != nil {
		return fmt.Errorf("failed to get logs: %w", err)
	}
	r.logger.InfoContext(r.ctx, "ranging through logs")
	for _, log := range logs {
		r.logger.InfoContext(r.ctx, fmt.Sprintf("setting term to: %d", log.Term))
		if err := r.setCurrentTerm(log.Term); err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}

		r.logger.InfoContext(r.ctx, fmt.Sprintf("applying log index: %d, term: %d", log.Index, log.Term))
		if err := r.sendLogToClient(log.Entry); err != nil {
			return fmt.Errorf("failed to send log to client: %w", err)
		}
		r.logger.InfoContext(r.ctx, fmt.Sprintf("setting last applied to: %d", log.Index))
		r.lastApplied = log.Index
	}
	return nil
}

func (r *Raft) sendLogToClient(entry []byte) error {
	errChan := make(chan error, 1)

	r.logger.InfoContext(r.ctx, "sending log to client")
	select {
	case r.sendLogsChan <- LogEntry{Entry: entry, ErrChan: errChan}:
		r.logger.InfoContext(r.ctx, "log sent to client, waiting for response")
		select {
		case <-r.ctx.Done():
			r.logger.InfoContext(r.ctx, "context cancelled while waiting for reply from raft")
			return context.Canceled
		case err := <-errChan:
			r.logger.InfoContext(r.ctx, "send log to client response received")
			return err
		}
	case <-r.ctx.Done():
		r.logger.InfoContext(r.ctx, "context cancelled while waiting for reply from raft")
		return context.Canceled
	}
}

// IsLeader returns true if the server is currently the leader, false otherwise
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.State == StateLeader
}

// LeaderKVAddress returns the KV gRPC address of the current leader,
// or an error if the leader is not found or there is an issue getting the leader id
func (r *Raft) LeaderKVAddress() (string, error) {
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
	return leader.KVAddress(), nil
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
	return leader.KVAddress(), nil
}

// addToLog adds a new log entry to the log and sends append entries requests to peers
// to replicate the log entry
func (r *Raft) addToLog(entry []byte) error {
	prevLogIndex, err := r.lastLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get previous log index: %w", err)
	}
	term, err := r.currentTerm()
	if err != nil {
		return fmt.Errorf("add to log - failed to get current term: %w", err)
	}
	dbEntry := database.LogEntry{Index: prevLogIndex + 1, Term: term, Entry: entry}
	if err := r.appendLogs([]database.LogEntry{dbEntry}); err != nil {
		return fmt.Errorf("failed to append log: %w", err)
	}
	if err := r.sendAppendEntries([]database.LogEntry{dbEntry}, true); err != nil {
		if errors.Is(err, ErrQuorumNotReached) {
			// entry
			r.logger.InfoContext(r.ctx, "quorum not reached for log entry, removing log entry and returning error")
			if err := r.deleteLogsFromIndex(prevLogIndex + 1); err != nil {
				return fmt.Errorf("failed to delete log entry after quorum not reached: %w", err)
			}
		}
		return fmt.Errorf("failed to send append entries: %w", err)
	}
	if err := r.sendLogToClient(entry); err != nil {
		return fmt.Errorf("failed to send log to client: %w", err)
	}

	if err := r.setLastLogIndex(prevLogIndex + 1); err != nil {
		return fmt.Errorf("failed to set previous log index: %w", err)
	}
	if err := r.setLeaderCommit(prevLogIndex + 1); err != nil {
		return fmt.Errorf("failed to set leader commit: %w", err)
	}
	return nil
}

// func (r *Raft) setNextIndexOnPeers() error {
// 	lastLogIndex, err := r.lastLogIndex()
// 	if err != nil {
// 		return err
// 	}
// 	for _, peer := range r.peers {
// 		r.mu.Lock()
// 		peer.SetNextIndex(lastLogIndex + 1)
// 		r.mu.Unlock()
// 	}
// 	return nil
// }

// Run starts the main loop and goroutines for the server.
// It calles the candidate state function and leader state function
// based on the current state.
func (r *Raft) Run() {
	r.logger.InfoContext(r.ctx, "initiating log on run start")
	if err := r.initLog(); err != nil {
		r.logger.ErrorContext(r.ctx, "failed to init and apply log: "+err.Error())
		r.cancel()
		return
	}

	r.logger.InfoContext(r.ctx, "setting next index on peers")
	// r.setNextIndexOnPeers()

	for {
		select {
		case <-r.ctx.Done():
			r.logger.InfoContext(r.ctx, "cancel command received, closing.")
			return
		default:
			r.logger.InfoContext(r.ctx, "running raft")
			r.logger.InfoContext(r.ctx, "waiting for initilizaition cooldown")
			r.logger.InfoContext(r.ctx, "listening for entries")
			go r.listenForEntries()
			r.logger.InfoContext(r.ctx, "starting election timer")
			go r.electionTimer()
			runTicker := time.NewTicker(10 * time.Millisecond).C
			for {
				select {
				case <-r.ctx.Done():
					r.logger.InfoContext(r.ctx, "cancel command received, closing.")
					return
				case <-runTicker:
					r.mu.Lock()
					state := r.State
					r.mu.Unlock()

					switch state {
					case StateFollower:
						// slog.InfoContext(r.ctx, "follower state")
					case StateCandidate:
						r.mu.Lock()
						electionResetTime := r.electionResetTime
						electionTimeout := r.electionTimeout
						r.mu.Unlock()

						if time.Since(electionResetTime) >= electionTimeout {
							if err := r.candidateState(); err != nil {
								r.logger.ErrorContext(r.ctx, "error on candidate state: "+err.Error())
							}
						}
					case StateLeader:
						r.logger.InfoContext(r.ctx, "leader state")
						if err := r.leaderState(); err != nil {
							log.Fatal(err.Error())
						}
					}
				}
			}
		}
	}
}

func (r *Raft) listenForEntries() {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.InfoContext(r.ctx, "cancel command received, closing.")
			return
		case entry := <-r.receiveLogsChan:
			if err := r.addToLog(entry.Entry); err != nil {
				entry.ErrChan <- err
				break
			}
			entry.ErrChan <- nil
		}
	}
}

// leaderState runs the main loop for the leader state, sending heartbeats to peers at regular intervals
func (r *Raft) leaderState() error {
	heartbeatTicker := time.NewTicker(10 * time.Millisecond).C
	for {
		select {
		case <-r.ctx.Done():
			return nil
		case <-heartbeatTicker:
			// slog.InfoContext(r.ctx, "heartbeat tick received")
			r.mu.Lock()
			state := r.State
			heartbeatResetTime := r.heartbeatResetTime
			heartbeatTimeout := r.heartbeatTimeout
			r.mu.Unlock()

			if state != StateLeader {
				r.logger.InfoContext(r.ctx, "not in leader state, ignoring heartbeat tick")
				return nil
			}

			if time.Since(heartbeatResetTime) >= heartbeatTimeout {
				r.logger.InfoContext(r.ctx, "sending heartbeats to peers")
				if err := r.sendHeartBeats(); err != nil {
					return err
				}
				return nil
			}
		}
	}
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
	if r.State != StateLeader {
		r.logger.InfoContext(r.ctx, "not in leader state, cannot send append entries")
		r.mu.Unlock()
		return errors.New("cannot send append entries when not in leader state")
	}
	r.heartbeatResetTime = time.Now()
	r.mu.Unlock()

	currentTerm, err := r.currentTerm()
	if err != nil {
		return fmt.Errorf("send ape - failed to get current term: %w", err)
	}
	prevLogIndex, err := r.lastLogIndex()
	if err != nil {
		return fmt.Errorf("failed to get previous log index: %w", err)
	}
	lastLogTerm, err := r.lastLogTerm()
	if err != nil {
		return fmt.Errorf("failed to get previous log term: %w", err)
	}
	leaderCommit, err := r.leaderCommit()
	if err != nil {
		return fmt.Errorf("failed to get leader commit: %w", err)
	}

	quorum := 1 + ((len(r.peers) + 1) / 2)

	// empty
	psMu := &sync.Mutex{}
	peerSuccess := make(map[int]bool, len(r.peers))
	for _, p := range r.peers {
		peerSuccess[p.ID()] = false
	}

	wg := &sync.WaitGroup{}
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer *Peer, psMu *sync.Mutex, peerSuccess map[int]bool, entries []database.LogEntry) {
			defer wg.Done()
			r.logger.InfoContext(r.ctx, fmt.Sprintf("sending append entries request to peer: %d: %s", peer.ID(), peer.Address()))

			// peerNextIndex := peer.NextIndex()

			// if len(entries) == 0 {
			// 	if peerNextIndex != r.lastApplied {
			// 		sendLog, err := r.db.GetLog(peerNextIndex)
			// 		if err != nil {
			// 			r.logger.ErrorContext(r.ctx, "failed to get peer next index: "+err.Error())
			// 			return
			// 		}
			// 		entries = []database.LogEntry{{Term: currentTerm, Index: sendLog.Index, Entry: sendLog.Entry}}
			// 	}
			// }

			appendEntriesReq := AppendEntriesRequest{
				Term:         currentTerm,
				LeaderID:     r.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  lastLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			r.logger.InfoContext(r.ctx, fmt.Sprintf("append entries request: %+v", appendEntriesReq))
			tCtx, tCancel := context.WithTimeout(r.ctx, time.Second*1)
			defer tCancel()
			success, term, err := r.Client.AppendEntries(tCtx, *peer, appendEntriesReq)
			if err != nil {
				r.logger.ErrorContext(r.ctx, "failed to send append entries request to peer: "+err.Error())
				success = false
				term = -1
				// return
			}
			if term > currentTerm {
				r.logger.InfoContext(r.ctx, fmt.Sprintf("peer %d term is bigger than current term, turning into follower", peer.ID()))
				r.mu.Lock()
				r.State = StateFollower
				r.mu.Unlock()

				psMu.Lock()
				peerSuccess[peer.ID()] = false
				psMu.Unlock()

				return
			}
			r.logger.InfoContext(r.ctx, fmt.Sprintf("peer %d responded with success: %t and term: %d", peer.ID(), success, term))
			if !success {
				r.logger.InfoContext(r.ctx, fmt.Sprintf("peer %d returned false for success", peer.ID()))
				// TODO: implement retries

				// decrement the next index to replicate missing logs
				// peer.SetNextIndex(peer.NextIndex() - 1)

				return
			}
			psMu.Lock()
			peerSuccess[peer.ID()] = true
			psMu.Unlock()
		}(peer, psMu, peerSuccess, entries)
	}

	wg.Wait()

	if checkQuorum {
		r.logger.InfoContext(r.ctx, "checking if quorum was achieved for append entries")
		successCount := 1 // count self
		for _, success := range peerSuccess {
			if success {
				successCount++
			}
		}
		r.logger.InfoContext(r.ctx, fmt.Sprintf("append entries success count: %d, quorum needed: %d", successCount, quorum))
		if successCount < quorum {
			return ErrQuorumNotReached
		}
		if len(entries) > 0 {
			lastEntry := entries[len(entries)-1]
			if err := r.setLastLogIndex(lastEntry.Index); err != nil {
				return fmt.Errorf("failed to set previous log index: %w", err)
			}
			if err := r.setLastLogTerm(lastEntry.Term); err != nil {
				return fmt.Errorf("failed to set previous log term: %w", err)
			}
			if err := r.setLeaderCommit(lastEntry.Index); err != nil {
				return fmt.Errorf("failed to set leader commit: %w", err)
			}
		}
	}

	return nil
}
