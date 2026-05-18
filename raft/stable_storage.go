package raft

import (
	"strconv"

	"github.com/tashima42/raft/database"
)

const (
	currentTermKey  = "internal-current-term"
	votedForKey     = "internal-voted-for"
	lastLogIndexKey = "internal-last-log-index"
	lastLogTermKey  = "internal-last-log-term"
	leaderCommitKey = "internal-leader-commit"
	leaderIDKey     = "internal-leader-id"
)

func (r *Raft) currentTerm() (int, error) {
	value, err := r.db.GetRaftValue(currentTermKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

// setCurrentTerm sets the current term, but also resets the votedFor to a null value (-1)
// to prevent the server from always voting for the same candidate in the next term
func (r *Raft) setCurrentTerm(term int) error {
	return r.db.SetRaftValue(currentTermKey, strconv.Itoa(term))
}

func (r *Raft) votedFor() (int, error) {
	value, err := r.db.GetRaftValue(votedForKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

func (r *Raft) setVotedFor(votedForID int) error {
	return r.db.SetRaftValue(votedForKey, strconv.Itoa(votedForID))
}

func (r *Raft) appendLogs(logs []database.LogEntry) error {
	return r.db.AppendLogs(logs)
}

func (r *Raft) deleteLogsFromIndex(idx int) error {
	return r.db.DeleteLogsFromIndex(idx)
}

func (r *Raft) logs() ([]database.LogEntry, error) {
	return r.db.GetLogs()
}

func (r *Raft) setLastLogIndex(idx int) error {
	return r.db.SetRaftValue(lastLogIndexKey, strconv.Itoa(idx))
}

func (r *Raft) lastLogIndex() (int, error) {
	value, err := r.db.GetRaftValue(lastLogIndexKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

func (r *Raft) setLeaderID(leaderID int) error {
	return r.db.SetRaftValue(leaderIDKey, strconv.Itoa(leaderID))
}

func (r *Raft) leaderID() (int, error) {
	leaderIDStr, err := r.db.GetRaftValue(leaderIDKey)
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(leaderIDStr)
}

// func (r *Raft) setLastLogIndex(idx int) error {
// 	return r.db.SetRaftValue(lastLogIndexKey, strconv.Itoa(idx))
// }

// func (r *Raft) lastLogIndex() (int, error) {
// 	value, err := r.db.GetRaftValue(lastLogIndexKey)
// 	if err != nil {
// 		return -1, err
// 	}

// 	return strconv.Atoi(value)
// }

func (r *Raft) setLastLogTerm(term int) error {
	return r.db.SetRaftValue(lastLogTermKey, strconv.Itoa(term))
}

func (r *Raft) lastLogTerm() (int, error) {
	value, err := r.db.GetRaftValue(lastLogTermKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

func (r *Raft) setLeaderCommit(commitIDX int) error {
	return r.db.SetRaftValue(leaderCommitKey, strconv.Itoa(commitIDX))
}

func (r *Raft) leaderCommit() (int, error) {
	value, err := r.db.GetRaftValue(leaderCommitKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}
