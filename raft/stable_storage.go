package raft

import (
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

func (r *Raft) appendLogs(logs []database.LogEntry) error {
	return r.db.AppendLogs(logs)
}

func (r *Raft) deleteLogsFromIndex(idx int) error {
	return r.db.DeleteLogsFromIndex(idx)
}

func (r *Raft) logs() ([]database.LogEntry, error) {
	return r.db.GetLogs()
}

func (r *Raft) logCount() (int, error) {
	return r.db.CountLogs()
}
