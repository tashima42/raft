package raft

import (
	"strconv"

	"github.com/tashima42/raft/database"
)

const (
	currentTermKey = "current-term"
	votedForKey    = "voted-for"
)

func (r *Raft) currentTerm() (int, error) {
	value, err := r.db.GetRaftValue(currentTermKey)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(value)
}

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

func (r *Raft) logs() ([]database.LogEntry, error) {
	return r.db.GetLogs()
}

func (r *Raft) logCount() (int, error) {
	return r.db.CountLogs()
}
