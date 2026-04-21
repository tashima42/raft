// Package transport encapsulates server transports
package transport

import (
	"context"
	"fmt"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/proto"
	"github.com/tashima42/raft/raft"
)

type GRPCServer struct {
	proto.UnimplementedRaftServer
	Raft *raft.Raft
}

func (g *GRPCServer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	rar := raft.AppendEntriesRequest{
		Term:         int(req.Term),
		LeaderID:     int(req.LeaderID),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		LeaderCommit: int(req.LeaderCommit),
		Entries:      make([]database.LogEntry, len(req.Entries)),
	}

	for i, entry := range req.Entries {
		action, err := raft.KeyValActionItoa(int(entry.Action.Number()))
		if err != nil {
			return nil, fmt.Errorf("invalid action in log entry: %w", err)
		}
		rar.Entries[i] = database.LogEntry{
			Action: string(action),
			Key:    entry.Key,
			Value:  entry.Value,
		}
	}
	success, term, err := g.Raft.AppendEntries(rar)
	if err != nil {
		return nil, err
	}
	return &proto.AppendEntriesResponse{
		Term:    int32(term),
		Success: success,
	}, nil
}

func (g *GRPCServer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	rvr := raft.RequestVoteRequest{
		Term:         int(req.Term),
		CandidateID:  int(req.CandidateID),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	term, voteGranted, err := g.Raft.RequestVote(rvr)
	if err != nil {
		return nil, err
	}
	return &proto.RequestVoteResponse{
		Term:        int32(term),
		VoteGranted: voteGranted,
	}, nil
}
