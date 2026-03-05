package raft

import (
	"fmt"

	"github.com/tashima42/raft/database"
)

type Client interface {
	AppendEntries(peer Peer, req AppendEntriesRequest) (bool, int, error)
	RequestVote(peer Peer, req RequestVoteRequest) (int, bool, error)
}

type mockClient struct {
	Peers map[int]*Raft
}

type AppendEntriesRequest struct {
	Term         int                 `json:"term"`
	LeaderID     int                 `json:"leaderId"`
	PrevLogIndex int                 `json:"prevLogIndex"`
	PrevLogTerm  int                 `json:"prevLogTerm"`
	Entries      []database.LogEntry `json:"entries,omitempty"`
	LeaderCommit int                 `json:"leaderCommit"`
}

type RequestVoteRequest struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

func NewMockClient(peers map[int]*Raft) *mockClient {
	return &mockClient{
		Peers: peers,
	}
}

func (m *mockClient) AppendEntries(peer Peer, req AppendEntriesRequest) (bool, int, error) {
	raft, exists := m.Peers[peer.ID]
	if !exists {
		return false, -1, fmt.Errorf("peer not found, id: %d", peer.ID)
	}

	return raft.AppendEntries(req)
}

func (m *mockClient) RequestVote(peer Peer, req RequestVoteRequest) (int, bool, error) {
	raft, exists := m.Peers[peer.ID]
	if !exists {
		return -1, false, fmt.Errorf("peer not found, id: %d", peer.ID)
	}

	return raft.RequestVote(req)
}
