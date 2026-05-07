// Package keyval implements a key-value store backed by the raft protocol
package keyval

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/tashima42/raft/raft"
)

var ErrKeyValInvalidAction = errors.New("invalid keyVal action")

type KeyValAction string

const (
	SetAction    KeyValAction = "SET"
	DeleteAction KeyValAction = "DELETE"
)

type pack struct {
	Action KeyValAction `json:"action"`
	Key    string       `json:"key"`
	Value  string       `json:"value"`
}

type KeyVal struct {
	Store              map[string]string
	ReceiveEntriesChan <-chan raft.LogEntry
	SendEntriesChan    chan<- raft.LogEntry
	mu                 *sync.Mutex
}

func NewKeyVal(receiveEntriesChan <-chan raft.LogEntry, sendEntryChan chan<- raft.LogEntry) KeyVal {
	return KeyVal{
		Store:              make(map[string]string),
		ReceiveEntriesChan: receiveEntriesChan,
		SendEntriesChan:    sendEntryChan,
		mu:                 &sync.Mutex{},
	}
}

func (k *KeyVal) Run() {
	go func() {
		for entry := range k.ReceiveEntriesChan {
			var p pack
			if err := json.Unmarshal(entry.Entry, &p); err != nil {
				entry.ErrChan <- err
				break
			}

			k.mu.Lock()
			if err := k.Exec(p.Action, p.Key, p.Value); err != nil {
				entry.ErrChan <- err
				k.mu.Unlock()
				break
			}
			k.mu.Unlock()
		}
	}()
}

func (k *KeyVal) SendLogToRaft(p pack) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}

	errChan := make(chan error)

	k.mu.Lock()
	defer k.mu.Unlock()

	k.SendEntriesChan <- raft.LogEntry{
		Entry:   b,
		ErrChan: errChan,
	}

	err = <-errChan

	return err
}

func (k *KeyVal) Exec(action KeyValAction, key, value string) error {
	switch action {
	case SetAction:
		k.Set(key, value)
		return nil
	case DeleteAction:
		k.Delete(key)
		return nil
	}

	return ErrKeyValInvalidAction
}

func (k *KeyVal) Set(key, value string) {
	k.Store[key] = value
}

func (k *KeyVal) Get(key string) string {
	return k.Store[key]
}

func (k *KeyVal) Delete(key string) {
	k.Store[key] = ""
}

func KeyValActionAtoi(k KeyValAction) (int, error) {
	switch k {
	case SetAction:
		return 0, nil
	case DeleteAction:
		return 1, nil
	}
	return -1, errors.New("invalid action")
}

func KeyValActionItoa(i int) (KeyValAction, error) {
	switch i {
	case 0:
		return SetAction, nil
	case 1:
		return DeleteAction, nil
	}
	return SetAction, errors.New("invalid action index")
}
