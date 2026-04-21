package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/proto"
	"github.com/tashima42/raft/raft"
	"github.com/tashima42/raft/transport"
	"google.golang.org/grpc"
)

var Version = "dev"

// https://github.com/raeperd/kickstart.go/blob/main/main.go
func main() {
	if err := run(context.Background(), os.Stdout, Version); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, w io.Writer, version string) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(w, nil)))

	peersIDs, peersAddresses, peersAPIAddresses, port, apiPort, id, dbLocation, err := parseFlags()
	if err != nil {
		log.Fatal(err.Error())
	}

	peers := make([]*raft.Peer, len(peersIDs))

	for i, id := range peersIDs {
		peers[i] = raft.NewPeer(id, peersAddresses[i], peersAPIAddresses[i])
	}

	db, err := database.NewSQLite(dbLocation)
	if err != nil {
		return errors.New("failed to start raft: " + err.Error())
	}

	grpcClient, err := raft.NewGRPCClient(peers)
	if err != nil {
		return fmt.Errorf("failed to start grpcClient: %w", err)
	}

	r, err := raft.NewRaft(ctx, db, grpcClient, id, peers)
	if err != nil {
		return errors.New("failed to start raft: " + err.Error())
	}

	go r.Run()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to start tcp server on port: %w", err)
	}
	s := grpc.NewServer()
	grpcServer := &transport.GRPCServer{Raft: r}
	proto.RegisterRaftServer(s, grpcServer)

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", apiPort),
		Handler:           route(slog.Default(), version, r),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)

	go func() {
		slog.InfoContext(ctx, "grpc server started", slog.Uint64("port", uint64(port)), slog.String("version", version))
		if err := s.Serve(lis); err != nil {
			errChan <- fmt.Errorf("grpc server: %w", err)
		}
	}()

	go func() {
		slog.InfoContext(ctx, "server started", slog.Uint64("port", uint64(apiPort)), slog.String("version", version))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		slog.InfoContext(ctx, "shutting down server")

		if err := r.GracefullyShutDown(); err != nil {
			return errors.New("failed to gracefully shutdown raft: " + err.Error())
		}

		// Create a new context for shutdown with timeout
		ctx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		// Shutdown the HTTP server first
		if err := httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}

		// After server is shutdown, cancel the main context to close other resources
		cancel()
		return nil
	}
}

// parseFlags returns peersIDs, peersAddresses, port, id, dbLocation, error
func parseFlags() ([]int, []string, []string, int, int, int, string, error) {
	port := flag.Int("port", 6437, "port to run grpc server on")
	apiPort := flag.Int("api-port", 5437, "port to run http server on")
	sid := flag.Int("id", 1, "raft server id")
	peersIDsStr := flag.String("peers-ids", "", "comma separated ids. E.g: 1,2,3")
	peersAddressesStr := flag.String("peers-addresses", "", "comma separated addresses. e.g: localhost:6438,localhost:6439")
	peersAPIAddressesStr := flag.String("peers-api-addresses", "", "comma separated addresses. e.g: http://localhost:5438,http://localhost:5439")
	dbLocation := flag.String("db-location", "raft.db", "db location. e.g: raft-1.db")
	flag.Parse()

	if port == nil {
		return nil, nil, nil, -1, -1, -1, "", errors.New("empty port")
	}
	if apiPort == nil {
		return nil, nil, nil, -1, -1, -1, "", errors.New("empty api port")
	}
	if sid == nil {
		return nil, nil, nil, -1, -1, -1, "", errors.New("empty id")
	}
	if peersIDsStr == nil {
		return nil, nil, nil, -1, -1, -1, "", errors.New("empty peers ids")
	}
	if peersAddressesStr == nil {
		return nil, nil, nil, -1, -1, -1, "", errors.New("empty peers addresses")
	}
	if peersAPIAddressesStr == nil {
		return nil, nil, nil, -1, -1, -1, "", errors.New("empty peers api addresses")
	}

	pis := strings.Split(*peersIDsStr, ",")
	ads := strings.Split(*peersAddressesStr, ",")
	apids := strings.Split(*peersAPIAddressesStr, ",")

	if len(pis) != len(ads) || len(ads) != len(apids) {
		return nil, nil, nil, -1, -1, -1, "", errors.New("peers ids and peers addresses or peers api addresses have different lengths")
	}

	ids := make([]int, len(pis))

	for i, pid := range pis {
		id, err := strconv.Atoi(pid)
		if err != nil {
			return nil, nil, nil, -1, -1, -1, "", err
		}
		ids[i] = id
	}

	return ids, ads, apids, *port, *apiPort, *sid, *dbLocation, nil
}

// route sets up and returns an [http.Handler] for all the server routes.
// It is the single source of truth for all the routes.
// You can add custom [http.Handler] as needed.
func route(log *slog.Logger, version string, r *raft.Raft) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /health", handleGetHealth(version))
	// mux.HandleFunc("POST /entries", handleAppendEntries(r))
	// mux.HandleFunc("POST /request-vote", handleRequestVote(r))

	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /api/key/{key}", handleGetKeyValue(r))
	apiMux.HandleFunc("PUT /api/key", handleSetKeyValue(r))

	middleware := forwardToLeader(r)
	mux.Handle("/api/", middleware(apiMux))

	handler := accesslog(mux, log)
	handler = recovery(handler, log)
	return handler
}

func forwardToLeader(r *raft.Raft) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if r.IsLeader() {
				next.ServeHTTP(w, req)
				return
			}

			leaderAddr, err := r.LeaderAPIAddress()
			if err != nil {
				http.Error(w, "Failed to get leader address: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if leaderAddr == "" {
				http.Error(w, "Leader not found", http.StatusInternalServerError)
				return
			}

			targetURL := leaderAddr + req.URL.Path
			if req.URL.RawQuery != "" {
				targetURL += "?" + req.URL.RawQuery
			}

			http.Redirect(w, req, targetURL, http.StatusTemporaryRedirect)
		})
	}
}

// accesslog is a middleware that logs request and response details,
// including latency, method, path, query parameters, IP address, response status, and bytes sent.
func accesslog(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wr := responseRecorder{ResponseWriter: w}

		next.ServeHTTP(&wr, r)

		log.InfoContext(r.Context(), "accessed",
			slog.String("latency", time.Since(start).String()),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("query", r.URL.RawQuery),
			slog.String("ip", r.Header.Get("X-Real-IP")),
			slog.Int("status", wr.status),
			slog.Int("bytes", wr.numBytes))
	})
}

// recovery is a middleware that recovers from panics during HTTP handler execution and logs the error details.
// It must be the last middleware in the chain to ensure it captures all panics.
func recovery(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := responseRecorder{ResponseWriter: w}
		defer func() {
			err := recover()
			if err == nil {
				return
			}

			if err, ok := err.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				// Handle the abort gracefully
				return
			}

			stack := make([]byte, 1024)
			n := runtime.Stack(stack, true)

			log.ErrorContext(r.Context(), "panic!",
				slog.Any("error", err),
				slog.String("stack", string(stack[:n])),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("query", r.URL.RawQuery),
				slog.String("ip", r.RemoteAddr))

			if wr.status > 0 {
				// response was already sent, nothing we can do
				return
			}

			// send error response
			http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
		}()
		next.ServeHTTP(&wr, r)
	})
}

// responseRecorder is a wrapper around [http.ResponseWriter] that records the status and bytes written during the response.
// It implements the [http.ResponseWriter] interface by embedding the original ResponseWriter.
type responseRecorder struct {
	http.ResponseWriter
	status   int
	numBytes int
}

// Header implements the [http.ResponseWriter] interface.
func (re *responseRecorder) Header() http.Header {
	return re.ResponseWriter.Header()
}

// Write implements the [http.ResponseWriter] interface.
func (re *responseRecorder) Write(b []byte) (int, error) {
	re.numBytes += len(b)
	return re.ResponseWriter.Write(b)
}

// WriteHeader implements the [http.ResponseWriter] interface.
func (re *responseRecorder) WriteHeader(statusCode int) {
	re.status = statusCode
	re.ResponseWriter.WriteHeader(statusCode)
}

// https://grafana.com/blog/2024/02/09/how-i-write-http-services-in-go-after-13-years
func encode[T any](w http.ResponseWriter, _ *http.Request, status int, v T) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}

func decode[T any](r *http.Request) (T, error) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, fmt.Errorf("decode json: %w", err)
	}
	return v, nil
}
