package raft

import (
	"fmt"
)

// AppendEntries receives entries from the leader and checks if they are valid
// and returns a bool for success or error, the current term and an error
func (r *Raft) AppendEntries(req AppendEntriesRequest) (bool, int, error) {
	// reset election timeout and prevent server from starting new elections
	r.logger.InfoContext(r.ctx, fmt.Sprintf("received append entries request: %+v", req))

	currentTerm, err := r.currentTerm()
	if err != nil {
		return false, currentTerm, fmt.Errorf("rpc - failed to get current term: %w", err)
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	// (§5.1)
	if req.Term < currentTerm {
		r.logger.InfoContext(r.ctx, "request term is smaller than current term, replying false")
		return false, currentTerm, nil
	}
	r.logger.InfoContext(r.ctx, "request term is valid for append entries")

	// TODO: if there is a conflict with the existing log, delete the existing log and all that follow it
	if currentTerm != req.Term {
		r.logger.InfoContext(r.ctx, fmt.Sprintf("setting current term to %d", req.Term))
		if err := r.setCurrentTerm(req.Term); err != nil {
			return false, currentTerm, fmt.Errorf("failed to set current term: %w", err)
		}
		if err := r.setVotedFor(-1); err != nil {
			return false, currentTerm, fmt.Errorf("failed to set voted for: %w", err)
		}
		currentTerm = req.Term
	}

	// if req.PrevLogIndex
	lastLogTerm, err := r.lastLogTerm()
	if err != nil {
		return false, currentTerm, fmt.Errorf("failed to get previous log term: %w", err)
	}
	if lastLogTerm != req.PrevLogTerm {
		return false, currentTerm, nil
	}

	r.logger.InfoContext(r.ctx, "appending entries to log")
	// (§5.3)
	r.logger.InfoContext(r.ctx, "getting count log")
	lastLogIndex, err := r.lastLogIndex()
	if err != nil {
		r.logger.ErrorContext(r.ctx, "append entries - failed to get last log index: "+err.Error())
		return false, currentTerm, lastIndex, fmt.Errorf("failed to get last log index: %w", err)
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("last log index is: %d", lastLogIndex))
	if req.PrevLogIndex > lastLogIndex {
		r.logger.InfoContext(r.ctx, "previous log index from request is bigger than current last log index, replying false")
		return false, currentTerm, nil
	}

	if err := r.appendLogs(req.Entries); err != nil {
		return false, currentTerm, fmt.Errorf("failed to append logs: %w", err)
	}

	r.logger.InfoContext(r.ctx, "ranging through request entries")
	for _, entry := range req.Entries {
		r.logger.InfoContext(r.ctx, "executing log on keyvalue state machine")
		if err := r.sendLogToClient(entry.Entry); err != nil {
			r.logger.ErrorContext(r.ctx, "append entries - failed to execute log on client: "+err.Error())
			return false, currentTerm, lastIndex, fmt.Errorf("failed to exec operation on client: %w", err)
		}
	}

	if err := r.setLeaderID(req.LeaderID); err != nil {
		r.logger.ErrorContext(r.ctx, "append entries - failed to set leader id: "+err.Error())
		return false, currentTerm, lastIndex, fmt.Errorf("failed to set leader id: %w", err)
	}

	if err := r.setLeaderCommit(req.LeaderCommit); err != nil {
		r.logger.ErrorContext(r.ctx, "append entries - failed to set leader commit: "+err.Error())
		return false, currentTerm, lastIndex, fmt.Errorf("failed to set leader commit: %w", err)
	}
	if len(req.Entries) > 0 {
		lastEntry := req.Entries[len(req.Entries)-1]
		if err := r.setLastLogIndex(lastEntry.Index); err != nil {
			r.logger.ErrorContext(r.ctx, "append entries - failed to set last log index: "+err.Error())
			return false, currentTerm, lastIndex, fmt.Errorf("failed to set previous log index: %w", err)
		}
		if err := r.setLastLogTerm(lastEntry.Term); err != nil {
			r.logger.ErrorContext(r.ctx, "append entries - failed to set last log term: "+err.Error())
			return false, currentTerm, lastIndex, fmt.Errorf("failed to set previous log term: %w", err)
		}
	} else {
		r.logger.InfoContext(r.ctx, "append entries request has no new log entries")
	}

	r.resetElectionTimeout()
	r.logger.InfoContext(r.ctx, "replying true")
	return true, currentTerm, nil
}

func (r *Raft) RequestVote(req RequestVoteRequest) (int, bool, error) {
	r.logger.InfoContext(r.ctx, fmt.Sprintf("received request vote request: %+v", req))
	r.logger.InfoContext(r.ctx, "locking mutex")
	r.mu.Lock()
	defer r.mu.Unlock()

	currentTerm, err := r.currentTerm()
	if err != nil {
		r.logger.ErrorContext(r.ctx, "request vote - failed to get current term: "+err.Error())
		return -1, false, err
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	if req.Term < currentTerm {
		r.logger.InfoContext(r.ctx, fmt.Sprintf("req term smaller than current term: %d < %d", req.Term, currentTerm))
		return currentTerm, false, nil
	}
	if req.Term > currentTerm {
		r.logger.InfoContext(r.ctx, fmt.Sprintf("request term is newer, updating local term from %d to %d", currentTerm, req.Term))
		currentTerm = req.Term
		if err := r.setCurrentTerm(currentTerm); err != nil {
			r.logger.ErrorContext(r.ctx, "request vote - failed to set current term: "+err.Error())
			return currentTerm, false, fmt.Errorf("failed to set current term: %w", err)
		}
		if err := r.setVotedFor(-1); err != nil {
			r.logger.ErrorContext(r.ctx, "request vote - failed to reset voted for: "+err.Error())
			return currentTerm, false, fmt.Errorf("failed to set voted for: %w", err)
		}
		r.State = StateFollower
		r.resetElectionTimeoutLocked()
	}
	if req.Term == currentTerm {
		r.logger.InfoContext(r.ctx, "request vote term matches current term")
	}
	votedFor, err := r.votedFor()
	if err != nil {
		r.logger.ErrorContext(r.ctx, "request vote - failed to get voted for: "+err.Error())
		return -1, false, fmt.Errorf("failed to get voted for: %w", err)
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("vote stored: %d", votedFor))

	lastLogIndex, err := r.lastLogIndex()
	if err != nil {
		r.logger.ErrorContext(r.ctx, "request vote - failed to get last log index: "+err.Error())
		return currentTerm, false, fmt.Errorf("failed to get last log index: %w", err)
	}
	lastLogTerm, err := r.lastLogTerm()
	if err != nil {
		r.logger.ErrorContext(r.ctx, "request vote - failed to get last log term: "+err.Error())
		return currentTerm, false, fmt.Errorf("failed to get last log term: %w", err)
	}

	if votedFor < 0 || votedFor == req.CandidateID {
		r.logger.InfoContext(r.ctx, "candidate is eligible for vote evaluation")
		if req.Term < currentTerm {
			r.logger.InfoContext(r.ctx, "candidate's term is not as up to date as current server's term, voting false")
			return currentTerm, false, nil
		}
		if req.LastLogTerm < lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex) {
			r.logger.InfoContext(r.ctx, "candidate's log is not as up to date as current server's log, voting false")
			return currentTerm, false, nil
		}

		r.logger.InfoContext(r.ctx, fmt.Sprintf("voting true for: %d", req.CandidateID))
		err := r.setVotedFor(req.CandidateID)
		if err != nil {
			r.logger.ErrorContext(r.ctx, "request vote - failed to persist voted for candidate: "+err.Error())
		} else {
			r.logger.InfoContext(r.ctx, fmt.Sprintf("persisted vote for candidate: %d", req.CandidateID))
		}
		r.State = StateFollower
		r.resetElectionTimeoutLocked()

		return currentTerm, true, err
	}
	r.logger.InfoContext(r.ctx, "voting false")
	return currentTerm, false, nil
}
