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

		res := raft.AppendEntriesResponse{Term: term, Success: success}

		if err := encode(w, nil, http.StatusOK, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func handleRequestVote(r *raft.Raft) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		requestVotesReq, err := decode[raft.RequestVoteRequest](req)
		if err != nil {
			http.Error(w, "failed to decode req: "+err.Error(), http.StatusInternalServerError)
			return
		}

		term, voteGranted, err := r.RequestVote(requestVotesReq)
		if err != nil {
			http.Error(w, "failed to grant vote: "+err.Error(), http.StatusInternalServerError)
			return
		}

		res := raft.RequestVoteResponse{Term: term, VoteGranted: voteGranted}
		if err := encode(w, nil, http.StatusOK, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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

func handleSetKeyValue(r *raft.Raft) http.HandlerFunc {
	type requestBody struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	type responseBody struct {
		Success bool `json:"success"`
	}
	return func(w http.ResponseWriter, req *http.Request) {
		setKeyBody, err := decode[requestBody](req)
		if err != nil {
			http.Error(w, "failed to decode req: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if err := r.AddToLog(raft.SetAction, setKeyBody.Key, setKeyBody.Value); err != nil {
			http.Error(w, "failed to set key-value: "+err.Error(), http.StatusInternalServerError)
			return
		}

		res := responseBody{Success: true}

		if err := encode(w, nil, http.StatusOK, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
