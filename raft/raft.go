package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type Raft struct {
	state          *raftState
	storageFile    *os.File
	storageEncoder *gob.Encoder
	storageDecoder *gob.Decoder
	storageMu      *sync.Mutex
}

type raftState struct {
	Store       keyVal
	CurrentTerm int
	VotedFor    int
	Log         []LogRecord
}

type LogRecord struct {
	term  int
	Entry LogEntry
}

type LogEntry struct {
	Action KeyValAction `json:"action"`
	Key    string       `json:"key"`
	Value  string       `json:"value"`
}

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

func NewRaft() (*Raft, error) {
	f, err := os.OpenFile("store.gob", os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, errors.New("failed to open store file: " + err.Error())
	}

	encoder := gob.NewEncoder(f)
	decoder := gob.NewDecoder(f)

	raft := &Raft{
		state: &raftState{
			Store:       newKeyVal(),
			CurrentTerm: 0,
			VotedFor:    0,
			Log:         []LogRecord{},
		},
		storageFile:    f,
		storageEncoder: encoder,
		storageDecoder: decoder,
		storageMu:      &sync.Mutex{},
	}

	if err := raft.restoreState(); err != nil {
		return nil, errors.New("failed to restore state: " + err.Error())
	}

	if err := raft.initLog(); err != nil {
		return nil, errors.New("failed to init and apply log: " + err.Error())
	}

	return raft, nil
}

func (r *Raft) GracefullyShutDown() error {
	if err := r.saveState(); err != nil {
		return errors.New("failed to store state: " + err.Error())
	}
	if err := r.storageFile.Close(); err != nil {
		return errors.New("failed to close store file: " + err.Error())
	}
	return nil
}

func (r *Raft) saveState() error {
	r.storageMu.Lock()
	defer r.storageMu.Unlock()

	if err := r.storageEncoder.Encode(r.state); err != nil {
		return errors.New("failed to encode state: " + err.Error())
	}

	return nil
}

func (r *Raft) restoreState() error {
	r.storageMu.Lock()
	defer r.storageMu.Unlock()

	if err := r.storageDecoder.Decode(r.state); err != nil {
		if err != io.EOF {
			return errors.New("failed to decode state: " + err.Error())
		}
	}

	return nil
}

func (r *Raft) initLog() error {
	for _, record := range r.state.Log {
		r.state.CurrentTerm = record.term

		if err := r.state.Store.Exec(record.Entry.Action, record.Entry.Key, record.Entry.Value); err != nil {
			return errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	return nil
}

func (r *Raft) AppendEntries(req AppendEntriesRequest) error {
	fmt.Println("appending")
	r.state.CurrentTerm = req.Term
	logRecords := make([]LogRecord, len(req.Entries))
	for i, entry := range req.Entries {
		logRecords[i] = LogRecord{
			term:  req.Term,
			Entry: entry,
		}
		if err := r.state.Store.Exec(entry.Action, entry.Key, entry.Value); err != nil {
			return errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	r.state.Log = append(r.state.Log, logRecords...)

	return r.saveState()
}
