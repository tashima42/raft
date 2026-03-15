package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/tashima42/raft/database"
)

type Client interface {
	// AppendEntries sends entries from the leader to a peer and returns if it was successful, the peer's term and an error
	AppendEntries(peer Peer, req AppendEntriesRequest) (bool, int, error)
	// RequestVote communicates with a peer requesting a vote and returns the peer's term, if the vote was granted and an error
	RequestVote(peer Peer, req RequestVoteRequest) (int, bool, error)
}

type mockClient struct {
	Peers map[int]*Raft
}

type httpClient struct {
	client http.Client
}

type AppendEntriesRequest struct {
	Term         int                 `json:"term"`
	LeaderID     int                 `json:"leaderId"`
	PrevLogIndex int                 `json:"prevLogIndex"`
	PrevLogTerm  int                 `json:"prevLogTerm"`
	Entries      []database.LogEntry `json:"entries,omitempty"`
	LeaderCommit int                 `json:"leaderCommit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type RequestVoteRequest struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

func NewHTTPClient(c http.Client) *httpClient {
	return &httpClient{
		client: c,
	}
}

func (h *httpClient) AppendEntries(peer Peer, req AppendEntriesRequest) (success bool, term int, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return false, -1, err
	}

	r, err := http.NewRequest(http.MethodPost, peer.Address+"/entries", bytes.NewBuffer(body))
	if err != nil {
		return false, -1, err
	}
	res, err := h.client.Do(r)
	if err != nil {
		return false, -1, err
	}
	defer func() {
		if closeErr := errors.Join(res.Body.Close()); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	decoder := json.NewDecoder(res.Body)

	resBody := AppendEntriesResponse{}
	err = decoder.Decode(&resBody)

	return resBody.Success, resBody.Term, err
}

func (h *httpClient) RequestVote(peer Peer, req RequestVoteRequest) (term int, voteGranted bool, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return -1, false, err
	}

	r, err := http.NewRequest(http.MethodPost, peer.Address+"/request-vote", bytes.NewBuffer(body))
	if err != nil {
		return -1, false, err
	}

	res, err := h.client.Do(r)
	if err != nil {
		return -1, false, err
	}
	defer func() {
		if closeErr := errors.Join(res.Body.Close()); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	decoder := json.NewDecoder(res.Body)

	resBody := RequestVoteResponse{}
	err = decoder.Decode(&resBody)

	return resBody.Term, resBody.VoteGranted, err
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
