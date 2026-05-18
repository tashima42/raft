package raft

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClient struct {
	peers map[int]*grpcPeer
}

type grpcPeer struct {
	conn   *grpc.ClientConn
	client *proto.RaftClient
}

func NewGRPCClient(peers []*Peer) (*GRPCClient, error) {
	c := GRPCClient{
		map[int]*grpcPeer{},
	}
	for _, peer := range peers {
		gp := grpcPeer{}
		conn, err := grpc.NewClient(peer.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to peer id=%d address=%s: %v", peer.ID(), peer.Address(), err)
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

func (g *GRPCClient) AppendEntries(ctx context.Context, peer Peer, req AppendEntriesRequest) (bool, int, int, error) {
	rar := proto.AppendEntriesRequest{
		Term:         int32(req.Term),
		LeaderID:     int32(req.LeaderID),
		PrevLogIndex: int32(req.PrevLogIndex),
		PrevLogTerm:  int32(req.PrevLogTerm),
		Entries:      make([]*proto.LogEntry, len(req.Entries)),
		LeaderCommit: int32(req.LeaderCommit),
	}

	for i, entry := range req.Entries {
		re := proto.LogEntry{
			Term:  int32(entry.Term),
			Index: int32(entry.Index),
			Entry: entry.Entry,
		}
		rar.Entries[i] = &re
	}

	client := g.peers[peer.ID()].client
	if client == nil {
		return false, -1, -1, fmt.Errorf("client not found for peer id: %d", peer.ID())
	}
	r, err := (*client).AppendEntries(ctx, &rar)
	if err != nil {
		log.Printf("append entries rpc failed for peer id=%d: %v", peer.ID(), err)
		return false, -1, -1, fmt.Errorf("failed to send append entries: %w", err)
	}
	if r == nil {
		return false, -1, -1, errors.New("nil response from append entries")
	}
	return r.Success, int(r.Term), int(r.LastIndex), nil
}

func (g *GRPCClient) RequestVote(ctx context.Context, peer Peer, req RequestVoteRequest) (int, bool, error) {
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
	res, err := (*client).RequestVote(ctx, &rvr)
	if err != nil {
		return -1, false, fmt.Errorf("failed to send request vote rpc: %w", err)
	}
	if res == nil {
		return -1, false, errors.New("nil response from request vote")
	}
	return int(res.Term), res.VoteGranted, nil
}

func (g *GRPCClient) Close() error {
	for _, peer := range g.peers {
		if err := peer.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
