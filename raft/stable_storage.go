package raft

import (
	"strconv"

	"github.com/tashima42/raft/database"
)

const (
	currentTermKey = "current-term"
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

func (r *Raft) appendLogs(logs []database.LogEntry) error {
	return r.db.AppendLogs(logs)
}
