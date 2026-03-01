// Package database wraps stable storage
package database

import (
	"database/sql"
	"errors"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	location string
	db       *sql.DB
}

type LogEntry struct {
	Term   int    `json:"term"`
	Action string `json:"action"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func NewDatabase(location string) (*Database, error) {
	db := &Database{location: location}
	if err := db.open(); err != nil {
		return nil, err
	}
	if err := db.migrate(); err != nil {
		return nil, err
	}
	return db, nil
}

func (d *Database) open() error {
	db, err := sql.Open("sqlite3", d.location)
	if err != nil {
		return err
	}
	d.db = db
	return nil
}

func (d *Database) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS 'raft' (
			id INTEGER NOT NULL PRIMARY KEY,
			key TEXT NOT NULL UNIQUE,
			VALUE TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS 'logs' (
			id INTEGER NOT NULL PRIMARY KEY,
			action TEXT NOT NULL,
			term INTEGER NOT NULL,
			key TEXT NOT NULL,
			VALUE TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
	}

	for _, migration := range migrations {
		if _, err := d.db.Exec(migration); err != nil {
			return errors.New("failed to apply migration: " + err.Error())
		}
	}
	return nil
}

func (d *Database) SetRaftValue(key, value string) (err error) {
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

func (d *Database) GetRaftValue(key string) (string, error) {
	var value string
	err := d.db.QueryRow("SELECT value FROM raft WHERE key = ?", key).Scan(&value)
	return value, err
}

func (d *Database) AppendLogs(logs []LogEntry) (err error) {
	var sb strings.Builder
	args := []any{}

	if _, err := sb.WriteString("INSERT INTO logs(action, term, key, value) VALUES"); err != nil {
		return err
	}

	for i, log := range logs {
		if i != 0 {
			if _, err := sb.WriteString(","); err != nil {
				return err
			}
		}
		if _, err := sb.WriteString("(?, ?, ?, ?)"); err != nil {
			return err
		}

		args = append(args, log.Action, log.Term, log.Key, log.Value)
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

func (d *Database) Close() error {
	return d.db.Close()
}
