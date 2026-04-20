package raft

import (
	"strconv"

	"github.com/tashima42/raft/database"
)

const (
	currentTermKey  = "internal-current-term"
	votedForKey     = "internal-voted-for"
	prevLogIndexKey = "internal-prev-log-index"
	logIndexKey     = "internal-log-index"
	prevLogTermKey  = "internal-prev-log-term"
	leaderCommitKey = "internal-leader-commit"
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
	if err := r.db.SetRaftValue(currentTermKey, strconv.Itoa(term)); err != nil {
		return err
	}
	return r.setVotedFor(-1)
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

func (r *Raft) logs() ([]database.LogEntry, error) {
	return r.db.GetLogs()
}

func (r *Raft) prevLog() (database.LogEntry, error) {
	prevLogIdx, err := r.prevLogIndex()
	if err != nil {
		return database.LogEntry{}, err
	}
	log, err := r.db.GetLog(prevLogIdx)
	if err != nil {
		return database.LogEntry{}, err
	}
	return log, nil
}

func (r *Raft) logCount() (int, error) {
	return r.db.CountLogs()
}

func (r *Raft) setPrevLogIndex(idx int) error {
	return r.db.SetRaftValue(prevLogIndexKey, strconv.Itoa(idx))
}

func (r *Raft) prevLogIndex() (int, error) {
	value, err := r.db.GetRaftValue(prevLogIndexKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

//
// func (r *Raft) setLogIndex(idx int) error {
// 	return r.db.SetRaftValue(logIndexKey, strconv.Itoa(idx))
// }
//
// func (r *Raft) logIndex() (int, error) {
// 	value, err := r.db.GetRaftValue(logIndexKey)
// 	if err != nil {
// 		return -1, err
// 	}
//
// 	return strconv.Atoi(value)
// }

func (r *Raft) setPrevLogTerm(term int) error {
	return r.db.SetRaftValue(prevLogTermKey, strconv.Itoa(term))
}

func (r *Raft) prevLogTerm() (int, error) {
	value, err := r.db.GetRaftValue(prevLogTermKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

//
// func (r *Raft) setLeaderCommit(commitIDX int) error {
// 	return r.db.SetRaftValue(leaderCommitKey, strconv.Itoa(commitIDX))
// }
//
// func (r *Raft) leaderCommit() (int, error) {
// 	value, err := r.db.GetRaftValue(leaderCommitKey)
// 	if err != nil {
// 		return -1, err
// 	}
//
// 	return strconv.Atoi(value)
// }
