package raft

import (
	"errors"
	"fmt"
	"log/slog"
)

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
