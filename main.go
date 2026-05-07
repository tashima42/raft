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
	APIPort                       int
	ServerID                      int
	PeersIDs                      []int
	PeersAddresses                []string
	PeersAPIAddresses             []string
	DBLocation                    string
	InitializationCooldownSeconds int
}

var Version = "dev"

func main() {
	if err := run(context.Background(), os.Stdout, Version); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, w io.Writer, version string) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(w, nil)))

	serverConfig, err := parseFlags()
	if err != nil {
		log.Fatal(err.Error())
	}

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

	r, err := raft.NewRaft(ctx, db, grpcClient, serverConfig.ServerID, peers, serverConfig.InitializationCooldownSeconds, sendRaftLogsChan)
	if err != nil {
		return fmt.Errorf("failed to start raft: %w", err)
	}

	kv := keyval.NewKeyVal(r.C(), sendRaftLogsChan)

	kv.Get("key")

	go r.Run()

	ctx, _ = signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", serverConfig.Port))
	if err != nil {
		return fmt.Errorf("failed to start tcp server on port: %w", err)
	}
	s := grpc.NewServer()
	grpcServer := &transport.GRPCServer{Raft: r}
	proto.RegisterRaftServer(s, grpcServer)

	errChan := make(chan error, 1)

	go func() {
		slog.InfoContext(ctx, "grpc server started", slog.Uint64("port", uint64(serverConfig.Port)), slog.String("version", version))
		if err := s.Serve(lis); err != nil {
			errChan <- fmt.Errorf("grpc server: %w", err)
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		slog.InfoContext(ctx, "shutting down server")

		if err := r.GracefullyShutDown(); err != nil {
			return fmt.Errorf("failed to gracefully shutdown raft: %w", err)
		}

		return nil
	}
}

// parseFlags returns peersIDs, peersAddresses, port, id, dbLocation, error
func parseFlags() (ServerConfig, error) {
	port := flag.Int("port", 6437, "port to run grpc server on")
	apiPort := flag.Int("api-port", 5437, "port to run http server on")
	sid := flag.Int("id", 1, "raft server id")
	peersIDsStr := flag.String("peers-ids", "", "comma separated ids. E.g: 1,2,3")
	peersAddressesStr := flag.String("peers-addresses", "", "comma separated addresses. e.g: localhost:6438,localhost:6439")
	peersAPIAddressesStr := flag.String("peers-api-addresses", "", "comma separated addresses. e.g: http://localhost:5438,http://localhost:5439")
	dbLocation := flag.String("db-location", "raft.db", "db location. e.g: raft-1.db")
	initializationCooldownSeconds := flag.Int("initilization-cooldown", 5, "delay before starting the clusters")
	flag.Parse()

	if port == nil {
		return ServerConfig{}, errors.New("empty port")
	}
	if apiPort == nil {
		return ServerConfig{}, errors.New("empty api port")
	}
	if sid == nil {
		return ServerConfig{}, errors.New("empty id")
	}
	if peersIDsStr == nil {
		return ServerConfig{}, errors.New("empty peers ids")
	}
	if peersAddressesStr == nil {
		return ServerConfig{}, errors.New("empty peers addresses")
	}
	if peersAPIAddressesStr == nil {
		return ServerConfig{}, errors.New("empty peers api addresses")
	}

	if initializationCooldownSeconds == nil {
		return ServerConfig{}, errors.New("empty initilization cooldown")
	}

	pis := strings.Split(*peersIDsStr, ",")
	ads := strings.Split(*peersAddressesStr, ",")
	apids := strings.Split(*peersAPIAddressesStr, ",")

	if len(pis) != len(ads) || len(ads) != len(apids) {
		return ServerConfig{}, errors.New("peers ids and peers addresses or peers api addresses have different lengths")
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
		PeersAPIAddresses:             apids,
		Port:                          *port,
		APIPort:                       *apiPort,
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
