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
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tashima42/raft/database"
	"github.com/tashima42/raft/raft"
)

var Version = "dev"

// https://github.com/raeperd/kickstart.go/blob/main/main.go
func main() {
	if err := run(context.Background(), os.Stdout, os.LookupEnv, Version); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, w io.Writer, lookupEnv func(string) (string, bool), version string) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(w, nil)))

	peersIDs, peersAddresses, port, id, dbLocation, err := parseFlags()
	if err != nil {
		log.Fatal(err.Error())
	}

	peers := make([]raft.Peer, len(peersIDs))

	for i, id := range peersIDs {
		peers[i] = raft.Peer{ID: id, Address: peersAddresses[i]}
	}

	db, err := database.NewSQLite(dbLocation)
	if err != nil {
		return errors.New("failed to start raft: " + err.Error())
	}

	r, err := raft.NewRaft(ctx, db, raft.NewHTTPClient(http.Client{Timeout: time.Second * 10}), id, peers)
	if err != nil {
		return errors.New("failed to start raft: " + err.Error())
	}

	go r.Run()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           route(slog.Default(), version, r),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "server started", slog.Uint64("port", uint64(port)), slog.String("version", version))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
		if err := server.Shutdown(ctx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}

		// After server is shutdown, cancel the main context to close other resources
		cancel()
		return nil
	}
}

// parseFlags returns peersIDs, peersAddresses, port, id, dbLocation, error
func parseFlags() ([]int, []string, int, int, string, error) {
	port := flag.Int("port", 6437, "port to run http server on")
	sid := flag.Int("id", 1, "raft server id")
	peersIDsStr := flag.String("peers-ids", "", "comma separated ids. E.g: 1,2,3")
	peersAddressesStr := flag.String("peers-addresses", "", "comma separated addresses. e.g: http://localhost:6438,http://localhost:6439")
	dbLocation := flag.String("db-location", "raft.db", "db location. e.g: raft-1.db")
	flag.Parse()

	if port == nil {
		return nil, nil, -1, -1, "", errors.New("empty port")
	}
	if sid == nil {
		return nil, nil, -1, -1, "", errors.New("empty id")
	}
	if peersIDsStr == nil {
		return nil, nil, -1, -1, "", errors.New("empty peers ids")
	}
	if peersAddressesStr == nil {
		return nil, nil, -1, -1, "", errors.New("empty peers addresses")
	}

	pis := strings.Split(*peersIDsStr, ",")
	ads := strings.Split(*peersAddressesStr, ",")

	if len(pis) != len(ads) {
		return nil, nil, -1, -1, "", errors.New("peers ids and perrs addresses have different lengths")
	}

	ids := make([]int, len(pis))

	for i, pid := range pis {
		id, err := strconv.Atoi(pid)
		if err != nil {
			return nil, nil, -1, -1, "", err
		}
		ids[i] = id
	}

	return ids, ads, *port, *sid, *dbLocation, nil
}

// route sets up and returns an [http.Handler] for all the server routes.
// It is the single source of truth for all the routes.
// You can add custom [http.Handler] as needed.
func route(log *slog.Logger, version string, r *raft.Raft) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", handleGetHealth(version))
	mux.HandleFunc("POST /entries", handleAppendEntries(r))
	mux.HandleFunc("POST /request-vote", handleRequestVote(r))
	mux.HandleFunc("GET /key/{key}", handleGetKeyValue(r))

	handler := accesslog(mux, log)
	handler = recovery(handler, log)
	return handler
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
