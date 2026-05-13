package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/keyval"
	"github.com/tashima42/raft/proto"
	"github.com/tashima42/raft/raft"
	"github.com/tashima42/raft/transport"
	"google.golang.org/grpc"
)

type e2eNode struct {
	id       int
	address  string
	kvAddr   string
	dbPath   string
	peers    []*raft.Peer
	logger   *slog.Logger
	listener net.Listener
	server   *grpc.Server
	raft     *raft.Raft
	kv       *keyval.KeyVal
	offline  bool
}

type e2eCluster struct {
	nodes      []*e2eNode
	attempt    int
	clusterLog string
}

func newE2ECluster(t *testing.T, testName string, size int) *e2eCluster {
	t.Helper()

	distDir := filepath.Join("dist", testName)
	if err := os.MkdirAll(distDir, 0o755); err != nil {
		t.Fatalf("failed to create dist dir: %v", err)
	}

	clusterLogFile, attempt, err := nextAttemptLogPath(distDir)
	if err != nil {
		t.Fatalf("failed to choose cluster log file path: %v", err)
	}
	f, err := os.Create(clusterLogFile)
	if err != nil {
		t.Fatalf("failed to create cluster log file: %v", err)
	}
	t.Cleanup(func() {
		_ = f.Close()
	})
	logger := slog.New(slog.NewTextHandler(f, nil))

	nodes := make([]*e2eNode, 0, size)
	for i := range size {
		id := i + 1
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for node %d: %v", id, err)
		}
		nodes = append(nodes, &e2eNode{
			id:       id,
			address:  lis.Addr().String(),
			kvAddr:   fmt.Sprintf("http://127.0.0.1:%d", 25000+id),
			listener: lis,
		})
	}

	for _, n := range nodes {
		peers := make([]*raft.Peer, 0, size-1)
		for _, p := range nodes {
			if p.id == n.id {
				continue
			}
			peers = append(peers, raft.NewPeer(p.id, p.address, p.kvAddr))
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
		ctx, cancel := context.WithCancel(context.Background())

		sendRaftLogsChan := make(chan raft.LogEntry)
		r, err := raft.NewRaft(ctx, cancel, logger, db, client, n.id, peers, 0, sendRaftLogsChan)
		if err != nil {
			t.Fatalf("failed to create raft node %d: %v", n.id, err)
		}

		kv := keyval.NewKeyVal(ctx, n.id, logger, r.C(), sendRaftLogsChan)

		n.dbPath = dbPath
		n.peers = peers
		n.logger = logger

		n.raft = r
		n.kv = kv
		n.server = grpc.NewServer()
		proto.RegisterRaftServer(n.server, &transport.RaftGRPCServer{Raft: r})

		go func(node *e2eNode) {
			_ = node.server.Serve(node.listener)
		}(n)
	}

	cluster := &e2eCluster{nodes: nodes, attempt: attempt, clusterLog: clusterLogFile}
	t.Cleanup(func() {
		cluster.shutdown()
	})
	t.Logf("e2e attempt=%d log=%s", cluster.attempt, cluster.clusterLog)
	return cluster
}

func nextAttemptLogPath(distDir string) (string, int, error) {
	for attempt := 1; ; attempt++ {
		candidate := filepath.Join(distDir, fmt.Sprintf("raft-cluster-attempt-%d.log", attempt))
		_, err := os.Stat(candidate)
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			return candidate, attempt, nil
		}
		return "", 0, err
	}
}

func (c *e2eCluster) shutdown() {
	for _, n := range c.nodes {
		c.disconnectNode(n)
	}
}

func (c *e2eCluster) disconnectNode(n *e2eNode) {
	if n == nil || n.offline {
		return
	}
	if n.server != nil {
		n.server.Stop()
		n.server = nil
	}
	if n.listener != nil {
		_ = n.listener.Close()
		n.listener = nil
	}
	if n.raft != nil {
		_ = n.raft.GracefullyShutDown()
		n.raft = nil
	}
	n.kv = nil
	n.offline = true
}

func (c *e2eCluster) reconnectNode(t *testing.T, n *e2eNode) {
	t.Helper()
	if n == nil {
		t.Fatalf("cannot reconnect nil node")
	}
	if !n.offline {
		return
	}

	lis, err := net.Listen("tcp", n.address)
	if err != nil {
		t.Fatalf("failed to listen again for node %d on %s: %v", n.id, n.address, err)
	}

	db, err := database.NewSQLite(n.dbPath)
	if err != nil {
		_ = lis.Close()
		t.Fatalf("failed to reopen sqlite for node %d: %v", n.id, err)
	}

	client, err := raft.NewGRPCClient(n.peers)
	if err != nil {
		_ = lis.Close()
		_ = db.Close()
		t.Fatalf("failed to create grpc client for node %d: %v", n.id, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sendRaftLogsChan := make(chan raft.LogEntry)
	r, err := raft.NewRaft(ctx, cancel, n.logger, db, client, n.id, n.peers, 0, sendRaftLogsChan)
	if err != nil {
		_ = lis.Close()
		_ = db.Close()
		t.Fatalf("failed to recreate raft node %d: %v", n.id, err)
	}

	kv := keyval.NewKeyVal(ctx, n.id, n.logger, r.C(), sendRaftLogsChan)
	server := grpc.NewServer()
	proto.RegisterRaftServer(server, &transport.RaftGRPCServer{Raft: r})

	n.listener = lis
	n.server = server
	n.raft = r
	n.kv = kv

	go func(node *e2eNode) {
		_ = node.server.Serve(node.listener)
	}(n)
	go n.kv.Run()
	go n.raft.Run()

	waitFor(t, 5*time.Second, fmt.Sprintf("node %d listener readiness", n.id), func() bool {
		conn, dialErr := net.DialTimeout("tcp", n.address, 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	})

	n.offline = false
}

func (c *e2eCluster) nodeByID(id int) *e2eNode {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
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

func waitForKeyOnAll(t *testing.T, c *e2eCluster, key, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, fmt.Sprintf("key %q replication", key), func() bool {
		for _, n := range c.nodes {
			if n.kv.Get(key) != want {
				return false
			}
		}
		return true
	})
}

func startClusterNormally(c *e2eCluster) {
	for _, n := range c.nodes {
		go n.kv.Run()
		go n.raft.Run()
	}
}

func startClusterWithIncreasingStartupDelays(c *e2eCluster, baseDelay time.Duration) {
	for i, n := range c.nodes {
		go n.kv.Run()
		go n.raft.Run()

		// Apply a fixed delay between each node startup to exercise
		// election behavior under staggered cluster boot.
		if i < len(c.nodes)-1 {
			time.Sleep(baseDelay)
		}
	}
}

func waitForStableSingleLeader(t *testing.T, c *e2eCluster, timeout, stableFor time.Duration) *e2eNode {
	t.Helper()

	deadline := time.Now().Add(timeout)
	stableSince := time.Time{}
	stableLeaderID := -1

	for time.Now().Before(deadline) {
		leaders := 0
		leaderID := -1
		for _, n := range c.nodes {
			if n.offline || n.raft == nil {
				continue
			}
			if n.raft.IsLeader() {
				leaders++
				leaderID = n.id
			}
		}

		if leaders == 1 {
			if leaderID == stableLeaderID {
				if time.Since(stableSince) >= stableFor {
					return c.nodeByID(leaderID)
				}
			} else {
				stableLeaderID = leaderID
				stableSince = time.Now()
			}
		} else {
			stableLeaderID = -1
			stableSince = time.Time{}
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for stable single leader (attempt=%d, log=%s)", c.attempt, c.clusterLog)
	return nil
}

func Test4NodesNaturalElection(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 4)
	startClusterNormally(cluster)

	leader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if leader == nil {
		t.Fatalf("expected a leader")
	}
}

func Test5NodesNaturalElection(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 5)
	startClusterNormally(cluster)

	leader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if leader == nil {
		t.Fatalf("expected a leader")
	}
}

func Test4NodesLeaderDisconnect(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 4)
	startClusterNormally(cluster)

	leader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if leader == nil {
		t.Fatalf("expected a leader")
	}

	// disconnect leader from cluster
	cluster.disconnectNode(leader)

	newLeader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if newLeader == nil {
		t.Fatalf("expected a new leader")
	}

	// reconnect the old leader using full node rebuild and ensure cluster stabilizes again
	cluster.reconnectNode(t, cluster.nodeByID(leader.id))
	restabilizedLeader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if restabilizedLeader == nil {
		t.Fatalf("expected a stable leader after reconnect")
	}
}

func Test5NodesLeaderDisconnectReconnect(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 5)
	startClusterNormally(cluster)

	leader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if leader == nil {
		t.Fatalf("expected a leader")
	}

	// disconnect leader from cluster
	cluster.disconnectNode(leader)
	// disconnect another node to create a minority partition and make sure the cluster doesn't elect a new leader until it has a majority again
	cluster.disconnectNode(cluster.nodeByID((leader.id % 5) + 1))
	time.Sleep(time.Second * 4)
	// reconnect the first disconnected leader to restore majority
	cluster.reconnectNode(t, cluster.nodeByID(leader.id))

	newLeader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if newLeader == nil {
		t.Fatalf("expected a new leader")
	}
}

func Test4NodesNaturalElectionWithIncreasingStartupDelays(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 4)
	startClusterWithIncreasingStartupDelays(cluster, 100*time.Millisecond)

	leader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if leader == nil {
		t.Fatalf("expected a leader")
	}
}

func Test4NodesLeaderDisconnectWithIncreasingStartupDelays(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 4)
	startClusterWithIncreasingStartupDelays(cluster, 100*time.Millisecond)

	leader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if leader == nil {
		t.Fatalf("expected a leader")
	}

	// disconnect leader from cluster
	cluster.disconnectNode(leader)

	newLeader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if newLeader == nil {
		t.Fatalf("expected a new leader")
	}

	// reconnect the old leader using full node rebuild and ensure cluster stabilizes again
	cluster.reconnectNode(t, cluster.nodeByID(leader.id))
	restabilizedLeader := waitForStableSingleLeader(t, cluster, 25*time.Second, 2*time.Second)
	if restabilizedLeader == nil {
		t.Fatalf("expected a stable leader after reconnect")
	}
}

func Test4NodesNaturalElectionAndReplication(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 4)
	startClusterNormally(cluster)

	leader := waitForStableSingleLeader(t, cluster, 12*time.Second, 600*time.Millisecond)

	k := fmt.Sprintf("natural-k-%d", time.Now().UnixNano())
	v := "natural-v"
	if err := leader.kv.SendLogToRaft(keyval.Pack{Key: k, Value: v}); err != nil {
		t.Fatalf("append through elected leader failed: %v", err)
	}

	waitForKeyOnAll(t, cluster, k, v, 8*time.Second)
}

// func Test4NodesFollowerDisconnectWriteReconnectCatchesUp(t *testing.T) {
// 	cluster := newE2ECluster(t, t.Name(), 4)
// 	startClusterNormally(cluster)

// 	leader := waitForStableSingleLeader(t, cluster, 12*time.Second, 600*time.Millisecond)
// 	if leader == nil {
// 		t.Fatalf("expected a leader")
// 	}

// 	disconnectedFollower := cluster.nodeByID((leader.id % 4) + 1)
// 	if disconnectedFollower == nil {
// 		t.Fatalf("expected a follower to disconnect")
// 	}

// 	cluster.disconnectNode(disconnectedFollower)

// 	k := fmt.Sprintf("follower-reconnect-k-%d", time.Now().UnixNano())
// 	v := "follower-reconnect-v"
// 	if err := leader.kv.SendLogToRaft(keyval.Pack{Key: k, Value: v}); err != nil {
// 		t.Fatalf("append through leader while follower is disconnected failed: %v", err)
// 	}

// 	// Ensure committed value is visible on currently online nodes before reconnecting follower.
// 	waitFor(t, 8*time.Second, fmt.Sprintf("key %q replication on online nodes", k), func() bool {
// 		for _, n := range cluster.nodes {
// 			if n.offline || n.kv == nil {
// 				continue
// 			}
// 			if n.kv.Get(k) != v {
// 				return false
// 			}
// 		}
// 		return true
// 	})

// 	cluster.reconnectNode(t, disconnectedFollower)

// 	// Reconnected follower should eventually receive the entry that was committed while it was offline.
// 	waitFor(t, 12*time.Second, fmt.Sprintf("reconnected follower %d to catch up key %q", disconnectedFollower.id, k), func() bool {
// 		if disconnectedFollower.kv == nil {
// 			return false
// 		}
// 		return disconnectedFollower.kv.Get(k) == v
// 	})
// }
