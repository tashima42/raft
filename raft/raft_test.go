package raft

import (
	"context"
	"testing"
	"time"

	"github.com/tashima42/raft/database"
)

func TestNewRaft(t *testing.T) {
	raft, err := mockRaft()
	if err != nil {
		t.Error(err)
	}

	currentTerm, err := raft.currentTerm()
	if err != nil {
		t.Error(err)
	}
	if currentTerm != 0 {
		t.Errorf("current term must be 0, got %d instead", currentTerm)
	}
}

func TestAppendEntriesRequestedTermLower(t *testing.T) {
	raft, err := mockRaft()
	if err != nil {
		t.Error(err)
	}
	if err := raft.setCurrentTerm(1); err != nil {
		t.Error(err)
	}
	req := AppendEntriesRequest{
		Term:         0,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []database.LogEntry{},
		LeaderCommit: 0,
	}
	success, term, err := raft.AppendEntries(req)
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Error("success should be false, request term is smaller than current term")
	}
	if term != 1 {
		t.Errorf("term should be 1, got %d", term)
	}
}

func TestAppendEntriesRequestedTermHigher(t *testing.T) {
	raft, err := mockRaft()
	if err != nil {
		t.Error(err)
	}
	// raft state follower
	if err := raft.setCurrentTerm(2); err != nil {
		t.Error(err)
	}
	req := AppendEntriesRequest{
		Term:         2,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []database.LogEntry{},
		LeaderCommit: 0,
	}
	success, term, err := raft.AppendEntries(req)
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Error("success should be true")
	}
	if term != 2 {
		t.Errorf("term should be 2, got %d", term)
	}
}

func TestElectionTimeout(t *testing.T) {
	t.Log("create a mock raft")
	raft, err := mockRaft()
	if err != nil {
		t.Error(err)
	}
	t.Log("verify follower state")
	if raft.State != StateFollower {
		t.Error("expected initial state is follower, got " + raft.State.String())
	}
	t.Log("start election timer")
	go raft.electionTimer()
	t.Log("set the timeout to zero")
	raft.setElectionTimeout(time.Duration(0))

	t.Log("wait for the election tick")
	select {
	case <-raft.electionTick:
		if raft.State != StateCandidate {
			t.Error("exptected state after the election timeout is candidate, got " + raft.State.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestRequestVote(t *testing.T) {
	// raft, err := mockRaft()
	// if err != nil {
	// 	t.Error(err)
	// }
	// raft.State = StateCandidate
	//
	// raftPeer2, err := mockRaft()
	// if err != nil {
	// 	t.Error(err)
	// }
	//
	// raftPeer3, err := mockRaft()
	// if err != nil {
	// 	t.Error(err)
	// }
	//
	// raft.peers = []Peer{{ID: 2, Address: "local:2"}, {ID: 3, Address: "local:3"}}
	// raft.Client = NewMockClient(map[int]*Raft{2: raftPeer2, 3: raftPeer3})
}

func mockRaft() (*Raft, error) {
	mockDB := database.NewMockDB()
	id := 1
	peers := []Peer{
		{
			ID:      2,
			Address: "local:2",
		},
		{
			ID:      3,
			Address: "local:3",
		},
	}
	return NewRaft(context.TODO(), mockDB, NewMockClient(nil), id, peers)
}
