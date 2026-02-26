package raft

import (
	"encoding/gob"
	"errors"
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
	term   int
	Action KeyValAction
	Key    string
	Value  string
}

func NewRaft() (*Raft, error) {
	f, err := os.OpenFile("store.gob", os.O_RDWR|os.O_CREATE, 0644)
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
	if err := r.storeState(); err != nil {
		return errors.New("failed to store state: " + err.Error())
	}
	if err := r.storageFile.Close(); err != nil {
		return errors.New("failed to close store file: " + err.Error())
	}
	return nil
}

func (r *Raft) storeState() error {
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

		if err := r.state.Store.Exec(record.Action, record.Key, record.Value); err != nil {
			return errors.New("failed to exec operation on keyVal store: " + err.Error())
		}
	}
	return nil
}

func (r *Raft) AppendToLog(record LogRecord) {
	record.term = r.state.CurrentTerm
	r.state.Log = append(r.state.Log, record)
}
