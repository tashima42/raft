// Package keyval implements a key-value store backed by the raft protocol
package keyval

import (
	"encoding/json"
	"sync"

	"github.com/tashima42/raft/raft"
)

type Pack struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KeyVal struct {
	Store              map[string]string
	receiveEntriesChan <-chan raft.LogEntry
	sendEntriesChan    chan<- raft.LogEntry
	mu                 *sync.Mutex
}

func NewKeyVal(receiveEntriesChan <-chan raft.LogEntry, sendEntryChan chan<- raft.LogEntry) *KeyVal {
	return &KeyVal{
		Store:              make(map[string]string),
		receiveEntriesChan: receiveEntriesChan,
		sendEntriesChan:    sendEntryChan,
		mu:                 &sync.Mutex{},
	}
}

func (k *KeyVal) Run() {
	go func() {
		for entry := range k.receiveEntriesChan {
			var p Pack
			if err := json.Unmarshal(entry.Entry, &p); err != nil {
				entry.ErrChan <- err
				break
			}

			k.mu.Lock()
			k.Set(p.Key, p.Value)
			k.mu.Unlock()
		}
	}()
}

func (k *KeyVal) SendLogToRaft(p Pack) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}

	errChan := make(chan error)

	k.mu.Lock()
	defer k.mu.Unlock()

	k.sendEntriesChan <- raft.LogEntry{
		Entry:   b,
		ErrChan: errChan,
	}

	err = <-errChan

	return err
}

func (k *KeyVal) Set(key, value string) {
	k.Store[key] = value
}

func (k *KeyVal) Get(key string) string {
	return k.Store[key]
}
