// Package keyval implements a key-value store backed by the raft protocol
package keyval

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/tashima42/raft/raft"
)

type Pack struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KeyVal struct {
	ctx                context.Context
	id                 int
	Store              map[string]string
	logger             *slog.Logger
	receiveEntriesChan <-chan raft.LogEntry
	sendEntriesChan    chan<- raft.LogEntry
	mu                 *sync.Mutex
}

func NewKeyVal(ctx context.Context, id int, logger *slog.Logger, receiveEntriesChan <-chan raft.LogEntry, sendEntryChan chan<- raft.LogEntry) *KeyVal {
	kvLogger := logger.With("node_id", id, "kv", true)
	return &KeyVal{
		ctx:                ctx,
		id:                 id,
		logger:             kvLogger,
		Store:              make(map[string]string),
		receiveEntriesChan: receiveEntriesChan,
		sendEntriesChan:    sendEntryChan,
		mu:                 &sync.Mutex{},
	}
}

func (k *KeyVal) Run() {
	for {
		select {
		case <-k.ctx.Done():
			k.logger.InfoContext(k.ctx, "cancel command received, shutting down kv loop")
			return
		case entry := <-k.receiveEntriesChan:
			k.logger.InfoContext(k.ctx, "new raft log received, executing: ", slog.Any("entry", entry))
			var p Pack
			if err := json.Unmarshal(entry.Entry, &p); err != nil {
				k.logger.ErrorContext(k.ctx, "error unmarshalling json: "+err.Error())
				entry.ErrChan <- err
				break
			}

			k.set(p.Key, p.Value)
			entry.ErrChan <- nil
		}
	}
}

func (k *KeyVal) SendLogToRaft(p Pack) error {
	k.logger.InfoContext(k.ctx, "sending log to raft")
	b, err := json.Marshal(p)
	if err != nil {
		k.logger.ErrorContext(k.ctx, "failed to marshal key-value operation: "+err.Error())
		return err
	}

	errChan := make(chan error, 1)

	select {
	case k.sendEntriesChan <- raft.LogEntry{Entry: b, ErrChan: errChan}:
		k.logger.InfoContext(k.ctx, "send succeeded, waiting for reply")
		select {
		case <-k.ctx.Done():
			k.logger.ErrorContext(k.ctx, "context cancelled while waiting for reply from raft")
			return context.Canceled
		case err = <-errChan:
			if err != nil {
				k.logger.ErrorContext(k.ctx, "error received from raft: "+err.Error())
			}
			return err
		}
	case <-k.ctx.Done():
		k.logger.ErrorContext(k.ctx, "context cancelled while waiting for reply from raft")
		return context.Canceled
	}
}

func (k *KeyVal) set(key, value string) {
	k.logger.InfoContext(k.ctx, "setting key-value pair", slog.String("key", key), slog.String("value", value))
	k.mu.Lock()
	defer k.mu.Unlock()
	k.Store[key] = value
}

func (k *KeyVal) Get(key string) string {
	k.logger.InfoContext(k.ctx, "getting value for key", slog.String("key", key))
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.Store[key]
}
