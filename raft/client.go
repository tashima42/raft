package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	// AppendEntries sends entries from the leader to a peer and returns if it was successful, the peer's term and an error
	AppendEntries(peer Peer, req AppendEntriesRequest) (bool, int, error)
	// RequestVote communicates with a peer requesting a vote and returns the peer's term, if the vote was granted and an error
	RequestVote(peer Peer, req RequestVoteRequest) (int, bool, error)
	// Close closes all open connections with clients
	Close() error
}

type mockClient struct {
	Peers map[int]*Raft
}

type httpClient struct {
	client http.Client
}

type grpcClient struct {
	peers map[int]*grpcPeer
}

type grpcPeer struct {
	conn   *grpc.ClientConn
	client *proto.RaftClient
}

func NewGRPCClient(peers []*Peer) (*grpcClient, error) {
	c := grpcClient{
		map[int]*grpcPeer{},
	}
	for _, peer := range peers {
		gp := grpcPeer{}
		conn, err := grpc.NewClient(peer.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to peer, id: %d, address: %s, error: %w", peer.ID(), peer.Address(), err)
		}
		rc := proto.NewRaftClient(conn)
		gp.conn = conn
		gp.client = &rc
		c.peers[peer.ID()] = &gp
	}
	return &c, nil
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

	r, err := http.NewRequest(http.MethodPost, peer.Address()+"/entries", bytes.NewBuffer(body))
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

	r, err := http.NewRequest(http.MethodPost, peer.Address()+"/request-vote", bytes.NewBuffer(body))
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

func (h *httpClient) Close() error {
	return nil
}

func NewMockClient(peers map[int]*Raft) *mockClient {
	return &mockClient{
		Peers: peers,
	}
}

func (m *mockClient) AppendEntries(peer Peer, req AppendEntriesRequest) (bool, int, error) {
	raft, exists := m.Peers[peer.ID()]
	if !exists {
		return false, -1, fmt.Errorf("peer not found, id: %d", peer.ID())
	}

	return raft.AppendEntries(req)
}

func (m *mockClient) RequestVote(peer Peer, req RequestVoteRequest) (int, bool, error) {
	raft, exists := m.Peers[peer.ID()]
	if !exists {
		return -1, false, fmt.Errorf("peer not found, id: %d", peer.ID())
	}

	return raft.RequestVote(req)
}

func (m *mockClient) Close() error {
	return nil
}

func (g *grpcClient) AppendEntries(peer Peer, req AppendEntriesRequest) (bool, int, error) {
	rar := proto.AppendEntriesRequest{
		Term:         int32(req.Term),
		LeaderID:     int32(req.LeaderID),
		PrevLogIndex: int32(req.PrevLogIndex),
		PrevLogTerm:  int32(req.PrevLogTerm),
		Entries:      make([]*proto.LogEntry, len(req.Entries)),
		LeaderCommit: int32(req.LeaderCommit),
	}

	for i, entry := range req.Entries {
		action, err := KeyValActionAtoi(KeyValAction(entry.Action))
		if err != nil {
			return false, -1, fmt.Errorf("invalid action in log entry: %w", err)
		}
		re := proto.LogEntry{
			Term:   int32(entry.Term),
			Index:  int32(entry.Index),
			Action: proto.Action(action),
			Key:    entry.Key,
			Value:  entry.Value,
		}
		rar.Entries[i] = &re
	}

	client := g.peers[peer.ID()].client
	if client == nil {
		return false, -1, fmt.Errorf("client not found for peer id: %d", peer.ID())
	}
	r, err := (*client).AppendEntries(context.TODO(), &rar)
	if err != nil {
		return false, -1, errors.New("failed to send append entries: " + err.Error())
	}
	if r == nil {
		return false, -1, errors.New("nil response from append entries")
	}
	return r.Success, int(r.Term), nil
}

func (g *grpcClient) RequestVote(peer Peer, req RequestVoteRequest) (int, bool, error) {
	rvr := proto.RequestVoteRequest{
		Term:         int32(req.Term),
		CandidateID:  int32(req.CandidateID),
		LastLogIndex: int32(req.LastLogIndex),
		LastLogTerm:  int32(req.LastLogTerm),
	}

	gPeer, found := g.peers[peer.ID()]
	if !found {
		return -1, false, fmt.Errorf("peer not found for id: %d", peer.ID())
	}
	client := gPeer.client
	if client == nil {
		return -1, false, fmt.Errorf("client not found for peer id: %d", peer.ID())
	}
	r, err := (*client).RequestVote(context.TODO(), &rvr)
	if err != nil {
		return -1, false, fmt.Errorf("failed to send request vote rpc: %w", err)
	}
	if r == nil {
		return -1, false, errors.New("nil response from request vote")
	}
	return int(r.Term), r.VoteGranted, nil
}

func (g *grpcClient) Close() error {
	for _, peer := range g.peers {
		if err := peer.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
