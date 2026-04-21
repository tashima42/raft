package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tashima42/raft/database"
)

type testCluster struct {
	nodes []*Raft
	byID  map[int]*Raft
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()

	byID := make(map[int]*Raft, size)
	nodes := make([]*Raft, 0, size)
	addresses := make(map[int]string, size)
	for i := range size {
		id := i + 1
		addresses[id] = fmt.Sprintf("local:%d", id)
	}

	for i := range size {
		id := i + 1
		peers := make([]*Peer, 0, size-1)
		for j := range size {
			peerID := j + 1
			if peerID == id {
				continue
			}
			peers = append(peers, NewPeer(peerID, addresses[peerID]))
		}

		node, err := NewRaft(context.Background(), database.NewMockDB(), NewMockClient(nil), id, peers)
		if err != nil {
			t.Fatalf("failed to create raft node %d: %v", id, err)
		}
		nodes = append(nodes, node)
		byID[id] = node
	}

	for _, node := range nodes {
		node.Client = NewMockClient(cloneNodeMap(byID))
	}

	return &testCluster{nodes: nodes, byID: byID}
}

func cloneNodeMap(src map[int]*Raft) map[int]*Raft {
	out := make(map[int]*Raft, len(src))
	for id, node := range src {
		out[id] = node
	}
	return out
}

func forceElection(t *testing.T, node *Raft) {
	t.Helper()
	node.State = StateCandidate
	if err := node.candidateState(); err != nil {
		t.Fatalf("candidateState failed for node %d: %v", node.id, err)
	}
	if !node.IsLeader() {
		t.Fatalf("node %d did not become leader", node.id)
	}
}

func waitFor(t *testing.T, timeout time.Duration, predicate func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitForValueOnAll(t *testing.T, nodes []*Raft, key, want string) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool {
		for _, node := range nodes {
			if node.KeyVal.Get(key) != want {
				return false
			}
		}
		return true
	}, fmt.Sprintf("all nodes to have key %q with value %q", key, want))
}

func disconnectPeerFromNode(t *testing.T, node *Raft, peerID int) {
	t.Helper()
	mock, ok := node.Client.(*mockClient)
	if !ok {
		t.Fatalf("node %d has non-mock client", node.id)
	}
	delete(mock.Peers, peerID)
}

func reconnectPeerToNode(t *testing.T, node *Raft, peerID int, peer *Raft) {
	t.Helper()
	mock, ok := node.Client.(*mockClient)
	if !ok {
		t.Fatalf("node %d has non-mock client", node.id)
	}
	mock.Peers[peerID] = peer
}

func TestIntegration3NodesAppendEntriesAndRead(t *testing.T) {
	cluster := newTestCluster(t, 3)
	leader := cluster.nodes[0]
	forceElection(t, leader)

	if err := leader.AddToLog(SetAction, "alpha", "1"); err != nil {
		t.Fatalf("leader append failed: %v", err)
	}
	waitForValueOnAll(t, cluster.nodes, "alpha", "1")

	if err := leader.AddToLog(SetAction, "alpha", "2"); err != nil {
		t.Fatalf("leader append update failed: %v", err)
	}
	waitForValueOnAll(t, cluster.nodes, "alpha", "2")
}

func TestIntegration3NodesPeerDisconnectAsIs(t *testing.T) {
	cluster := newTestCluster(t, 3)
	leader := cluster.nodes[0]
	forceElection(t, leader)

	disconnectedPeerID := cluster.nodes[2].id
	disconnectedPeer := cluster.nodes[2]
	disconnectPeerFromNode(t, leader, disconnectedPeerID)

	err := leader.AddToLog(SetAction, "beta", "1")
	if err == nil {
		t.Fatalf("expected append to fail when one peer is disconnected in current implementation")
	}

	if got := leader.KeyVal.Get("beta"); got != "" {
		t.Fatalf("expected leader state machine to not apply failed append, got value %q", got)
	}

	reconnectPeerToNode(t, leader, disconnectedPeerID, disconnectedPeer)
	if err := leader.AddToLog(SetAction, "beta", "2"); err != nil {
		t.Fatalf("expected append to succeed after reconnect, got error: %v", err)
	}
	waitForValueOnAll(t, cluster.nodes, "beta", "2")
}

func TestIntegration5NodesReelectionAndAppend(t *testing.T) {
	cluster := newTestCluster(t, 5)
	firstLeader := cluster.nodes[0]
	forceElection(t, firstLeader)

	firstTerm, err := firstLeader.currentTerm()
	if err != nil {
		t.Fatalf("failed to read first leader term: %v", err)
	}

	// Simulate leader loss using existing state controls, then force a new election.
	firstLeader.State = StateFollower
	newLeader := cluster.nodes[1]
	forceElection(t, newLeader)

	newTerm, err := newLeader.currentTerm()
	if err != nil {
		t.Fatalf("failed to read new leader term: %v", err)
	}
	if newTerm <= firstTerm {
		t.Fatalf("expected new term greater than old term, old=%d new=%d", firstTerm, newTerm)
	}

	if err := newLeader.AddToLog(SetAction, "gamma", "x"); err != nil {
		t.Fatalf("append after reelection failed: %v", err)
	}
	waitForValueOnAll(t, cluster.nodes, "gamma", "x")
}

func TestIntegration5NodesDisconnectBehaviorAsIs(t *testing.T) {
	cluster := newTestCluster(t, 5)
	leader := cluster.nodes[0]
	forceElection(t, leader)

	disconnectPeerFromNode(t, leader, cluster.nodes[4].id)

	if err := leader.AddToLog(SetAction, "delta", "1"); err == nil {
		t.Fatalf("expected append to fail with disconnected peer in current implementation")
	}
}
