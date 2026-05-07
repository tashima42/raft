package main

import (
	"context"
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
	apiAddr  string
	listener net.Listener
	server   *grpc.Server
	raft     *raft.Raft
	kv       *keyval.KeyVal
}

type e2eCluster struct {
	nodes []*e2eNode
}

func newE2ECluster(t *testing.T, testName string, size int) *e2eCluster {
	t.Helper()

	distDir := filepath.Join("dist", testName)
	if err := os.MkdirAll(distDir, 0o755); err != nil {
		t.Fatalf("failed to create dist dir: %v", err)
	}

	clusterLogFile := filepath.Join(distDir, "raft-cluster.log")
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
		ctx, cancel := context.WithCancel(context.Background())

		sendRaftLogsChan := make(chan raft.LogEntry)
		r, err := raft.NewRaft(ctx, cancel, logger, db, client, n.id, peers, 0, sendRaftLogsChan)
		if err != nil {
			t.Fatalf("failed to create raft node %d: %v", n.id, err)
		}

		kv := keyval.NewKeyVal(r.C(), sendRaftLogsChan)

		n.raft = r
		n.kv = kv
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

// func waitFor(t *testing.T, timeout time.Duration, what string, fn func() bool) {
// 	t.Helper()
// 	deadline := time.Now().Add(timeout)
// 	for time.Now().Before(deadline) {
// 		if fn() {
// 			return
// 		}
// 		time.Sleep(20 * time.Millisecond)
// 	}
// 	t.Fatalf("timed out waiting for %s", what)
// }
//
// func waitForKeyOnAll(t *testing.T, c *e2eCluster, key, want string, timeout time.Duration) {
// 	t.Helper()
// 	waitFor(t, timeout, fmt.Sprintf("key %q replication", key), func() bool {
// 		for _, n := range c.nodes {
// 			if n.kv.Get(key) != want {
// 				return false
// 			}
// 		}
// 		return true
// 	})
// }

func startClusterNormally(c *e2eCluster) {
	for _, n := range c.nodes {
		n.kv.Run()
		go n.raft.Run()
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

	t.Fatalf("timed out waiting for stable single leader")
	return nil
}

func Test4NodesNaturalElection(t *testing.T) {
	cluster := newE2ECluster(t, t.Name(), 4)
	startClusterNormally(cluster)

	leader := waitForStableSingleLeader(t, cluster, 15*time.Second, 600*time.Millisecond)
	if leader == nil {
		t.Fatalf("expected a leader")
	}
}

// func TestGRPCE2E4NodesNaturalElectionAndReplication(t *testing.T) {
// 	cluster := newE2ECluster(t, t.Name(), 4)
// 	startClusterNormally(cluster)
//
// 	leader := waitForStableSingleLeader(t, cluster, 12*time.Second, 600*time.Millisecond)
//
// 	k := fmt.Sprintf("natural-k-%d", time.Now().UnixNano())
// 	v := "natural-v"
// 	if err := leader.kv.SendLogToRaft(keyval.Pack{Key: k, Value: v}); err != nil {
// 		t.Fatalf("append through elected leader failed: %v", err)
// 	}
//
// 	waitForKeyOnAll(t, cluster, k, v, 8*time.Second)
// }
