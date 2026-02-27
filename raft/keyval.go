// Package raft implements the raft protocol and a keyvalue state machine
package raft

import "errors"

var ErrKeyValInvalidAction = errors.New("invalid keyVal action")

type KeyValAction string

const (
	SetAction    KeyValAction = "SET"
	DeleteAction KeyValAction = "DELETE"
)

type keyVal struct {
	Store map[string]string `json:"store"`
}

func newKeyVal() keyVal {
	return keyVal{Store: make(map[string]string)}
}

func (k *keyVal) Exec(action KeyValAction, key, value string) error {
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

func (k *keyVal) Set(key, value string) {
	k.Store[key] = value
}

func (k *keyVal) Get(key string) string {
	return k.Store[key]
}

func (k *keyVal) Delete(key string) {
	k.Store[key] = ""
}
