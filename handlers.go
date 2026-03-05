package main

import (
	"net/http"
	"time"

	"github.com/tashima42/raft/raft"
)

// handleGetHealth returns an [http.HandlerFunc] that responds with the health status of the service.
// It includes the service version, VCS revision, build time, and modified status.
// The service version can be set at build time using the VERSION variable (e.g., 'make build VERSION=v1.0.0').
func handleGetHealth(version string) http.HandlerFunc {
	type responseBody struct {
		Version string `json:"version"`
		Uptime  string `json:"uptime"`
	}

	res := responseBody{Version: version}

	up := time.Now()

	return func(w http.ResponseWriter, _ *http.Request) {
		res.Uptime = time.Since(up).String()

		if err := encode(w, nil, http.StatusOK, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func handleAppendEntries(r *raft.Raft) http.HandlerFunc {
	type responseBody struct {
		Term    int  `json:"term"`
		Success bool `json:"success"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		appendEntriesReq, err := decode[raft.AppendEntriesRequest](req)
		if err != nil {
			http.Error(w, "failed to decode req: "+err.Error(), http.StatusInternalServerError)
			return
		}
		success, term, err := r.AppendEntries(appendEntriesReq)
		if err != nil {
			http.Error(w, "failed to append entries: "+err.Error(), http.StatusInternalServerError)
			return
		}

		res := responseBody{Term: term, Success: success}

		if err := encode(w, nil, http.StatusOK, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func handleGetKeyValue(r *raft.Raft) http.HandlerFunc {
	type responseBody struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		key := req.PathValue("key")

		value := r.KeyVal.Get(key)
		if value == "" {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		res := responseBody{Key: key, Value: value}

		if err := encode(w, nil, http.StatusOK, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
