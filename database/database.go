// Package database wraps stable storage
package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type Database interface {
	open() error
	migrate() error
	SetRaftValue(key, value string) error
	GetRaftValue(key string) (string, error)
	AppendLogs(logs []LogEntry) error
	DeleteLogsFromIndex(idx int) error
	GetLogs() ([]LogEntry, error)
	GetLog(idx int) (LogEntry, error)
	Close() error
}

type LogEntry struct {
	Term  int
	Index int
	Entry []byte
}

type SQLite struct {
	location string
	db       *sql.DB
}

func NewSQLite(location string) (*SQLite, error) {
	db := &SQLite{location: location}
	if err := db.open(); err != nil {
		return nil, err
	}
	if err := db.migrate(); err != nil {
		return nil, err
	}
	return db, nil
}

func (d *SQLite) open() error {
	db, err := sql.Open("sqlite3", d.location)
	if err != nil {
		return err
	}
	d.db = db
	return nil
}

func (d *SQLite) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS 'raft' (
			id INTEGER NOT NULL PRIMARY KEY,
			key TEXT NOT NULL UNIQUE,
			value TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS 'logs' (
			id INTEGER NOT NULL PRIMARY KEY,
			idx INTEGER NOT NULL,
			term INTEGER NOT NULL,
			entry BLOB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
	}

	for _, migration := range migrations {
		if _, err := d.db.Exec(migration); err != nil {
			return fmt.Errorf("failed to apply migration: %w", err)
		}
	}
	return nil
}

func (d *SQLite) SetRaftValue(key, value string) (err error) {
	stmt, err := d.db.Prepare("INSERT INTO raft(key, value) VALUES(?, ?) ON CONFLICT (key) DO UPDATE SET value=?")
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := errors.Join(stmt.Close()); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	_, err = stmt.Exec(key, value, value)
	return err
}

func (d *SQLite) GetRaftValue(key string) (string, error) {
	var value string
	err := d.db.QueryRow("SELECT value FROM raft WHERE key = ?", key).Scan(&value)
	return value, err
}

func (d *SQLite) AppendLogs(logs []LogEntry) (err error) {
	if len(logs) < 1 {
		return nil
	}
	var sb strings.Builder
	args := []any{}

	if _, err := sb.WriteString("INSERT INTO logs(term, entry, idx) VALUES"); err != nil {
		return err
	}

	for i, entry := range logs {
		if i != 0 {
			if _, err := sb.WriteString(","); err != nil {
				return err
			}
		}
		if _, err := sb.WriteString("(?, ?, ?)"); err != nil {
			return err
		}

		args = append(args, entry.Term, entry.Entry, entry.Index)
	}
	stmt, err := d.db.Prepare(sb.String())
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := errors.Join(stmt.Close()); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	_, err = stmt.Exec(args...)
	return err
}

func (d *SQLite) DeleteLogsFromIndex(idx int) (err error) {
	query := "DELETE FROM logs WHERE idx > ?"

	_, err = d.db.Exec(query, idx)
	if err != nil {
		return fmt.Errorf("failed to delete logs from index: %w", err)
	}

	return nil
}

func (d *SQLite) GetLogs() (logs []LogEntry, err error) {
	rows, err := d.db.Query("SELECT term, entry, idx FROM logs;")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		entry := LogEntry{}
		err = rows.Scan(&entry.Term, &entry.Entry, &entry.Index)
		if err != nil {
			return nil, err
		}
		logs = append(logs, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := errors.Join(rows.Close()); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	return logs, err
}

func (d *SQLite) GetLog(idx int) (log LogEntry, err error) {
	var term int
	var entry []byte
	err = d.db.QueryRow("SELECT term, entry FROM logs WHERE idx = ?", idx).Scan(&term, &entry)
	log.Entry = entry
	log.Term = term
	log.Index = idx
	return log, err
}

func (d *SQLite) Close() error {
	return d.db.Close()
}
