// Package database wraps stable storage
package database

import (
	"database/sql"
)

type Database interface {
	open() error
	migrate() error
	SetRaftValue(key, value string) error
	GetRaftValue(key string) (string, error)
	AppendLogs(logs []LogEntry) error
	GetLogs() ([]LogEntry, error)
	GetLog(idx int) (LogEntry, error)
	CountLogs() (int, error)
	Close() error
}

type LogEntry struct {
	Term   int    `json:"term"`
	Index  int    `json:"index"`
	Action string `json:"action"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

type MockDB struct {
	logs []LogEntry
	raft map[string]string
}

func NewMockDB() *MockDB {
	return &MockDB{
		logs: []LogEntry{},
		raft: map[string]string{},
	}
}

func (m *MockDB) open() error {
	return nil
}

func (m *MockDB) migrate() error {
	return nil
}

func (m *MockDB) SetRaftValue(key, value string) error {
	m.raft[key] = value
	return nil
}

func (m *MockDB) GetRaftValue(key string) (string, error) {
	value, found := m.raft[key]
	if !found {
		return "", sql.ErrNoRows
	}
	return value, nil
}

func (m *MockDB) AppendLogs(logs []LogEntry) error {
	m.logs = append(m.logs, logs...)
	return nil
}

func (m *MockDB) GetLogs() ([]LogEntry, error) {
	return m.logs, nil
}

func (m *MockDB) GetLog(idx int) (LogEntry, error) {
	if len(m.logs) <= idx {
		return LogEntry{}, sql.ErrNoRows
	}
	return m.logs[idx], nil
}

func (m *MockDB) CountLogs() (int, error) {
	return len(m.logs), nil
}

func (m *MockDB) Close() error {
	return nil
}
