package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/keyval"
	"github.com/tashima42/raft/proto"
	"github.com/tashima42/raft/raft"
	"github.com/tashima42/raft/transport"
	"google.golang.org/grpc"
)

type ServerConfig struct {
	Port                          int
	KVPort                        int
	ServerID                      int
	PeersIDs                      []int
	PeersAddresses                []string
	PeersKVAddresses              []string
	DBLocation                    string
	InitializationCooldownSeconds int
	LogLocation                   string
}

var Version = "dev"

func main() {
	if err := run(Version); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(version string) error {
	serverConfig, err := parseFlags()
	if err != nil {
		log.Fatal(err.Error())
	}

	var w io.Writer

	if serverConfig.LogLocation == "stdout" {
		w = os.Stdout
	} else {
		f, err := os.OpenFile(serverConfig.LogLocation, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
		if err != nil {
			log.Fatal(err)
		}
		w = io.MultiWriter(os.Stdout, f)
	}

	logger := slog.New(slog.NewTextHandler(w, nil))

	peers := make([]*raft.Peer, len(serverConfig.PeersIDs))

	for i, id := range serverConfig.PeersIDs {
		peers[i] = raft.NewPeer(id, serverConfig.PeersAddresses[i], serverConfig.PeersAddresses[i])
	}

	db, err := database.NewSQLite(serverConfig.DBLocation)
	if err != nil {
		return fmt.Errorf("failed to start raft: %w", err)
	}

	grpcClient, err := raft.NewGRPCClient(peers)
	if err != nil {
		return fmt.Errorf("failed to start grpcClient: %w", err)
	}

	sendRaftLogsChan := make(chan raft.LogEntry)

	ctx, cancel := context.WithCancel(context.Background())
	r, err := raft.NewRaft(ctx, cancel, logger, db, grpcClient, serverConfig.ServerID, peers, serverConfig.InitializationCooldownSeconds, sendRaftLogsChan)
	if err != nil {
		return fmt.Errorf("failed to start raft: %w", err)
	}

	kv := keyval.NewKeyVal(ctx, serverConfig.ServerID, logger, r.C(), sendRaftLogsChan)

	go kv.Run()
	go r.Run()

	ctx, _ = signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	raftLis, err := net.Listen("tcp", fmt.Sprintf(":%d", serverConfig.Port))
	if err != nil {
		return fmt.Errorf("failed to start tcp server on port: %w", err)
	}
	raftServer := grpc.NewServer()
	raftGRPCServer := &transport.RaftGRPCServer{Raft: r}
	proto.RegisterRaftServer(raftServer, raftGRPCServer)

	kvLis, err := net.Listen("tcp", fmt.Sprintf(":%d", serverConfig.KVPort))
	if err != nil {
		return fmt.Errorf("failed to start tcp server on port: %w", err)
	}
	kvServer := grpc.NewServer()
	kvGRPCServer := &transport.KeyValGRPCServer{KeyVal: kv}
	proto.RegisterKeyValServer(kvServer, kvGRPCServer)

	errChan := make(chan error, 1)

	go func() {
		logger.InfoContext(ctx, "raft grpc server started", slog.Uint64("port", uint64(serverConfig.Port)), slog.String("version", version))
		if err := raftServer.Serve(raftLis); err != nil {
			errChan <- fmt.Errorf("raft grpc server: %w", err)
		}
	}()

	go func() {
		logger.InfoContext(ctx, "kv grpc server started", slog.Uint64("port", uint64(serverConfig.KVPort)), slog.String("version", version))
		if err := kvServer.Serve(kvLis); err != nil {
			errChan <- fmt.Errorf("kv grpc server: %w", err)
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		logger.InfoContext(ctx, "shutting down server")

		if err := r.GracefullyShutDown(); err != nil {
			return fmt.Errorf("failed to gracefully shutdown raft: %w", err)
		}

		return nil
	}
}

// parseFlags returns peersIDs, peersAddresses, port, id, dbLocation, error
func parseFlags() (ServerConfig, error) {
	port := flag.Int("port", 6437, "port to run grpc server on")
	kvPort := flag.Int("kv-port", 5437, "port to run http server on")
	sid := flag.Int("id", 1, "raft server id")
	logFile := flag.String("log-location", "stdout", "raft-{id}.log")
	peersIDsStr := flag.String("peers-ids", "", "comma separated ids. E.g: 1,2,3")
	peersAddressesStr := flag.String("peers-addresses", "", "comma separated addresses. e.g: localhost:6438,localhost:6439")
	peersKVAddressesStr := flag.String("peers-kv-addresses", "", "comma separated addresses. e.g: http://localhost:5438,http://localhost:5439")
	dbLocation := flag.String("db-location", "raft.db", "db location. e.g: raft-1.db")
	initializationCooldownSeconds := flag.Int("initilization-cooldown", 5, "delay before starting the clusters")
	flag.Parse()

	if port == nil {
		return ServerConfig{}, errors.New("empty port")
	}
	if kvPort == nil {
		return ServerConfig{}, errors.New("empty kv port")
	}
	if sid == nil {
		return ServerConfig{}, errors.New("empty id")
	}
	if logFile == nil {
		return ServerConfig{}, errors.New("empty log file")
	}
	if peersIDsStr == nil {
		return ServerConfig{}, errors.New("empty peers ids")
	}
	if peersAddressesStr == nil {
		return ServerConfig{}, errors.New("empty peers addresses")
	}
	if peersKVAddressesStr == nil {
		return ServerConfig{}, errors.New("empty peers kv addresses")
	}

	if initializationCooldownSeconds == nil {
		return ServerConfig{}, errors.New("empty initilization cooldown")
	}

	pis := strings.Split(*peersIDsStr, ",")
	ads := strings.Split(*peersAddressesStr, ",")
	kvAddresses := strings.Split(*peersKVAddressesStr, ",")

	if len(pis) != len(ads) || len(ads) != len(kvAddresses) {
		return ServerConfig{}, errors.New("peers ids and peers addresses or peers kv addresses have different lengths")
	}

	ids := make([]int, len(pis))

	for i, pid := range pis {
		id, err := strconv.Atoi(pid)
		if err != nil {
			return ServerConfig{}, err
		}
		ids[i] = id
	}

	return ServerConfig{
		ServerID:                      *sid,
		PeersIDs:                      ids,
		PeersAddresses:                ads,
		PeersKVAddresses:              kvAddresses,
		LogLocation:                   *logFile,
		Port:                          *port,
		KVPort:                        *kvPort,
		DBLocation:                    *dbLocation,
		InitializationCooldownSeconds: *initializationCooldownSeconds,
	}, nil
}

// func forwardToLeader(r *raft.Raft) func(http.Handler) http.Handler {
// 	return func(next http.Handler) http.Handler {
// 		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
// 			if r.IsLeader() {
// 				next.ServeHTTP(w, req)
// 				return
// 			}
//
// 			leaderAddr, err := r.LeaderAPIAddress()
// 			if err != nil {
// 				http.Error(w, "Failed to get leader address: "+err.Error(), http.StatusInternalServerError)
// 				return
// 			}
// 			if leaderAddr == "" {
// 				http.Error(w, "Leader not found", http.StatusInternalServerError)
// 				return
// 			}
//
// 			targetURL := leaderAddr + req.URL.Path
// 			if req.URL.RawQuery != "" {
// 				targetURL += "?" + req.URL.RawQuery
// 			}
//
// 			http.Redirect(w, req, targetURL, http.StatusTemporaryRedirect)
// 		})
// 	}
// }
