// Package transport encapsulates server transports
package transport

import (
	"context"
	"log"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/proto"
	"github.com/tashima42/raft/raft"
)

type RaftGRPCServer struct {
	proto.UnimplementedRaftServer
	Raft *raft.Raft
}

func (g *RaftGRPCServer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	rar := raft.AppendEntriesRequest{
		Term:         int(req.Term),
		LeaderID:     int(req.LeaderID),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		LeaderCommit: int(req.LeaderCommit),
		Entries:      make([]database.LogEntry, len(req.Entries)),
	}

	for i, entry := range req.Entries {
		rar.Entries[i] = database.LogEntry{
			Term:  int(entry.Term),
			Index: int(entry.Index),
			Entry: entry.Entry,
		}
	}
	success, term, lastIndex, err := g.Raft.AppendEntries(rar)
	if err != nil {
		log.Printf("append entries handler failed: %v", err)
		return nil, err
	}
	return &proto.AppendEntriesResponse{
		Term:      int32(term),
		Success:   success,
		LastIndex: int32(lastIndex),
	}, nil
}

func (g *RaftGRPCServer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	rvr := raft.RequestVoteRequest{
		Term:         int(req.Term),
		CandidateID:  int(req.CandidateID),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	term, voteGranted, err := g.Raft.RequestVote(rvr)
	if err != nil {
		log.Printf("request vote handler failed: %v", err)
		return nil, err
	}
	return &proto.RequestVoteResponse{
		Term:        int32(term),
		VoteGranted: voteGranted,
	}, nil
}
