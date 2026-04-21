package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/proto"
	"github.com/tashima42/raft/raft"
	"github.com/tashima42/raft/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type e2eNode struct {
	id       int
	address  string
	apiAddr  string
	listener net.Listener
	server   *grpc.Server
	raft     *raft.Raft
}

type e2eCluster struct {
	nodes []*e2eNode
}

func newE2ECluster(t *testing.T, size int) *e2eCluster {
	t.Helper()

	nodes := make([]*e2eNode, 0, size)
	addresses := make(map[int]string, size)

	for i := range size {
		id := i + 1
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for node %d: %v", id, err)
		}
		addresses[id] = lis.Addr().String()
		nodes = append(nodes, &e2eNode{
			id:       id,
			address:  lis.Addr().String(),
			apiAddr:  fmt.Sprintf("http://127.0.0.1:%d", 25000+id),
			listener: lis,
		})
	}

	for _, n := range nodes {
		peers := make([]*raft.Peer, 0, size-1)
		for _, p := range nodes {
			if p.id == n.id {
				continue
			}
			peers = append(peers, raft.NewPeer(p.id, p.address, p.apiAddr))
		}

		dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("raft-node-%d.db", n.id))
		db, err := database.NewSQLite(dbPath)
		if err != nil {
			t.Fatalf("failed to create sqlite for node %d: %v", n.id, err)
		}

		client, err := raft.NewGRPCClient(peers)
		if err != nil {
			t.Fatalf("failed to create grpc client for node %d: %v", n.id, err)
		}

		r, err := raft.NewRaft(context.Background(), db, client, n.id, peers)
		if err != nil {
			t.Fatalf("failed to create raft node %d: %v", n.id, err)
		}

		n.raft = r
		n.server = grpc.NewServer()
		proto.RegisterRaftServer(n.server, &transport.GRPCServer{Raft: r})

		go func(node *e2eNode) {
			_ = node.server.Serve(node.listener)
		}(n)
	}

	cluster := &e2eCluster{nodes: nodes}
	t.Cleanup(func() {
		cluster.shutdown()
	})
	return cluster
}

func (c *e2eCluster) shutdown() {
	for _, n := range c.nodes {
		if n.server != nil {
			n.server.Stop()
		}
		if n.listener != nil {
			_ = n.listener.Close()
		}
		if n.raft != nil {
			_ = n.raft.GracefullyShutDown()
		}
	}
}

func (c *e2eCluster) nodeByID(id int) *e2eNode {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

func mustProtoClient(t *testing.T, address string) (proto.RaftClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create proto grpc client for %s: %v", address, err)
	}
	return proto.NewRaftClient(conn), conn
}

func waitFor(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitForSingleLeader(t *testing.T, c *e2eCluster, timeout time.Duration) *e2eNode {
	t.Helper()
	var leader *e2eNode
	waitFor(t, timeout, "single leader", func() bool {
		leaders := 0
		leader = nil
		for _, n := range c.nodes {
			if n.raft.IsLeader() {
				leaders++
				leader = n
			}
		}
		return leaders == 1
	})
	return leader
}

func waitForKeyOnAll(t *testing.T, c *e2eCluster, key, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, fmt.Sprintf("key %q replication", key), func() bool {
		for _, n := range c.nodes {
			if n.raft.KeyVal.Get(key) != want {
				return false
			}
		}
		return true
	})
}

func requestVotesFromAll(t *testing.T, c *e2eCluster, candidateID, term int) int {
	t.Helper()
	granted := 0
	for _, n := range c.nodes {
		if n.id == candidateID {
			continue
		}
		client, conn := mustProtoClient(t, n.address)
		res, err := client.RequestVote(context.Background(), &proto.RequestVoteRequest{
			Term:         int32(term),
			CandidateID:  int32(candidateID),
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
		_ = conn.Close()
		if err != nil {
			t.Fatalf("request vote failed for node %d: %v", n.id, err)
		}
		if res.VoteGranted {
			granted++
		}
	}
	return granted
}

func setNodeTermBySelfVote(t *testing.T, n *e2eNode, term int) {
	t.Helper()
	_, _, err := n.raft.RequestVote(raft.RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("failed to set term for node %d: %v", n.id, err)
	}
}

func TestGRPCE2E3NodesElectionAndAppend(t *testing.T) {
	cluster := newE2ECluster(t, 3)

	candidate := cluster.nodeByID(1)
	candidate.raft.State = raft.StateCandidate
	votesGranted := requestVotesFromAll(t, cluster, candidate.id, 1)
	if votesGranted < 2 {
		t.Fatalf("expected quorum votes for candidate %d, got %d", candidate.id, votesGranted)
	}
	setNodeTermBySelfVote(t, candidate, 1)
	candidate.raft.State = raft.StateLeader

	leader := waitForSingleLeader(t, cluster, 2*time.Second)
	if err := leader.raft.AddToLog(raft.SetAction, "e2e-k", "v1"); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	waitForKeyOnAll(t, cluster, "e2e-k", "v1", 2*time.Second)
}

func TestGRPCE2EAppendEntriesRPC(t *testing.T) {
	cluster := newE2ECluster(t, 3)

	follower := cluster.nodeByID(2)
	client, conn := mustProtoClient(t, follower.address)
	defer func() {
		_ = conn.Close()
	}()

	okRes, err := client.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
		Term:         2,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
		Entries: []*proto.LogEntry{{
			Term:   2,
			Index:  1,
			Action: proto.Action_SET,
			Key:    "rpc-k",
			Value:  "rpc-v",
		}},
	})
	if err != nil {
		t.Fatalf("append entries rpc failed: %v", err)
	}
	if !okRes.Success {
		t.Fatalf("expected append entries success=true")
	}

	waitFor(t, 2*time.Second, "follower to apply append entries value", func() bool {
		return follower.raft.KeyVal.Get("rpc-k") == "rpc-v"
	})

	staleRes, err := client.AppendEntries(context.Background(), &proto.AppendEntriesRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
		Entries:      []*proto.LogEntry{},
	})
	if err != nil {
		t.Fatalf("stale append entries rpc failed: %v", err)
	}
	if staleRes.Success {
		t.Fatalf("expected stale append entries success=false")
	}
}

func TestGRPCE2E5NodesReelectionAndAppend(t *testing.T) {
	cluster := newE2ECluster(t, 5)

	leader1 := cluster.nodeByID(1)
	leader1.raft.State = raft.StateCandidate
	votesGranted1 := requestVotesFromAll(t, cluster, leader1.id, 1)
	if votesGranted1 < 3 {
		t.Fatalf("expected quorum votes for first leader candidate, got %d", votesGranted1)
	}
	setNodeTermBySelfVote(t, leader1, 1)
	leader1.raft.State = raft.StateLeader
	waitForSingleLeader(t, cluster, 2*time.Second)

	if err := leader1.raft.AddToLog(raft.SetAction, "before", "one"); err != nil {
		t.Fatalf("append before reelection failed: %v", err)
	}
	waitForKeyOnAll(t, cluster, "before", "one", 2*time.Second)

	leader1.raft.State = raft.StateFollower

	leader2 := cluster.nodeByID(2)
	leader2.raft.State = raft.StateCandidate
	votesGranted2 := requestVotesFromAll(t, cluster, leader2.id, 2)
	if votesGranted2 < 3 {
		t.Fatalf("expected quorum votes for second leader candidate, got %d", votesGranted2)
	}
	setNodeTermBySelfVote(t, leader2, 2)
	leader2.raft.State = raft.StateLeader

	waitForSingleLeader(t, cluster, 2*time.Second)
	if err := leader2.raft.AddToLog(raft.SetAction, "after", "two"); err != nil {
		t.Fatalf("append after reelection failed: %v", err)
	}
	waitForKeyOnAll(t, cluster, "after", "two", 2*time.Second)
}

func TestGRPCE2ERequestVoteRPC(t *testing.T) {
	cluster := newE2ECluster(t, 3)

	node := cluster.nodeByID(2)
	client, conn := mustProtoClient(t, node.address)
	defer func() {
		_ = conn.Close()
	}()

	grantedRes, err := client.RequestVote(context.Background(), &proto.RequestVoteRequest{
		Term:         3,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("request vote rpc failed: %v", err)
	}
	if !grantedRes.VoteGranted {
		t.Fatalf("expected vote to be granted for higher term")
	}

	staleRes, err := client.RequestVote(context.Background(), &proto.RequestVoteRequest{
		Term:         2,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("stale request vote rpc failed: %v", err)
	}
	if staleRes.VoteGranted {
		t.Fatalf("expected vote not granted for stale term")
	}
}
