// Package raft implements the raft consensus module
package raft

import (
	"context"
	"math/rand"

	"github.com/tashima42/raft/proto"
)

type RaftState int

const (
	RaftStateLeader RaftState = iota
	RaftStateFollower
	RaftStateCandidate
)

type RaftMessageKind int

const (
	RaftMessageRequestVotesRequest RaftMessageKind = iota
	RaftMessageRequestVoteResponse
	RaftMessageAppendEntriesRequest
	RaftMessageAppendEntriesResponse
	RaftMessageHeartbeatsRequest
	RaftMessageHeartbeatsResponse
	RaftMessageTick
)

type RaftMessage struct {
	Kind RaftMessageKind

	From uint64
	To   uint64

	Term    uint64
	LogTerm uint64
	Index   uint64
	Commit  uint64

	Vote       uint64
	Reject     bool
	RejectHint uint64

	Entries []proto.LogEntry
}

type Raft struct {
	ctx context.Context

	id uint64

	term         uint64
	lastLogIndex uint64
	state        RaftState

	minElectionTimeout uint64
	maxElectionTimeout uint64
	electionTimeout    uint64
	heartbeatTimeout   uint64
	electionTicks      uint64
	heartbeatTicks     uint64

	hasReady bool

	votes          map[string]bool
	replicatedLogs map[string]bool

	stepc    chan RaftMessage
	tickc    chan struct{}
	readyc   chan RaftMessage
	advancec chan struct{}
}

func NewRaft(ctx context.Context, id, minElectionTimeout, maxElectionTimeout, heartbeatTimeout uint64) *Raft {
	raft := Raft{
		ctx:                ctx,
		id:                 id,
		minElectionTimeout: minElectionTimeout,
		maxElectionTimeout: maxElectionTimeout,
		electionTimeout:    0,
		heartbeatTimeout:   heartbeatTimeout,
	}

	raft.resetElectionTimeout()

	return &raft
}

func (r *Raft) Run() {
	var readyc chan RaftMessage
	var rm RaftMessage
	for {
		if r.hasReady && readyc == nil {
			readyc = r.readyc
		}
		select {
		case <-r.ctx.Done():
			return
		case <-r.tickc:
			r.tick()
		}
	}
}

func (r *Raft) Ready() chan RaftMessage {
	return r.readyc
}

func (r *Raft) tick() {
	r.electionTicks += 1
	r.heartbeatTicks += 1

	if r.electionTicks >= r.electionTimeout {
		if r.state == RaftStateLeader {
			return
		}
		r.state = RaftStateCandidate
		r.resetElectionTimeout()
	}

	if r.heartbeatTicks >= r.heartbeatTimeout {
		if r.state != RaftStateLeader {
			return
		}
		r.heartbeatTicks = 0
		// r.readyc <- ClientEventMessage{kind: RaftMessageHeartbeatsRequest}
	}
}

func (r *Raft) resetElectionTimeout() {
	randomTimeout := uint64(rand.Intn(int(r.maxElectionTimeout)-int(r.minElectionTimeout)+1) + int(r.minElectionTimeout))
	r.electionTimeout = randomTimeout
}
