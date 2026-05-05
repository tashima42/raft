package raft

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

var (
	minimumElectionTimeoutMS int64 = 300
	maximumElectionTimeoutMS int64 = 2 * minimumElectionTimeoutMS
)

// resetElectionTimeout resets the election timeout to a random valuebetween
// the minimum and maximum election timeout values
func (r *Raft) resetElectionTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	slog.InfoContext(r.ctx, "resetting election timeout")
	r.electionTimeout = r.randomElectionTimeout()
	slog.InfoContext(r.ctx, fmt.Sprintf("new election timeout is: %s", r.electionTimeout.String()))
	r.electionResetTime = time.Now()
}

func (r *Raft) randomElectionTimeout() time.Duration {
	randTimeout := rand.Int64N(maximumElectionTimeoutMS - minimumElectionTimeoutMS)
	return time.Millisecond * time.Duration(minimumElectionTimeoutMS+randTimeout)
}

// electionTimer creates a ticker that checks if the election timeout has been reached
// and transitions the server to candidate state if it has.
func (r *Raft) electionTimer() {
	slog.InfoContext(r.ctx, "running election timer")

	electionTicker := time.NewTicker(10 * time.Millisecond).C

	for range electionTicker {
		r.mu.Lock()
		if r.State == StateLeader {
			slog.InfoContext(r.ctx, "in leader state, ignoring election tick")
			r.mu.Unlock()
			return
		}
		if time.Since(r.electionResetTime) >= r.electionTimeout {
			slog.InfoContext(r.ctx, "setting state to candidate and unlocking mutex")
			time.Sleep(r.randomElectionTimeout())
			r.State = StateCandidate
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

// requestVotes sends a RequestVote RPC to all peers and updates the server's state based
// on the results. If the server has achieved a majority of votes, it becomes the lader.
// If the server founds a peer with a higher term, it steps down to follower and updates
// its term to the higher term.
func (r *Raft) requestVotes() error {
	slog.InfoContext(r.ctx, "requesting votes, locking mutex")
	r.mu.Lock()
	defer r.mu.Unlock()
	currentTerm, err := r.currentTerm()
	if err != nil {
		return err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, "requesting vote from: "+peer.Address())
		// TODO: implement last log
		peerTerm, voteGranted, err := r.Client.RequestVote(*peer, RequestVoteRequest{Term: currentTerm, CandidateID: r.id, LastLogIndex: 0, LastLogTerm: 0})
		if err != nil {
			return err
		}
		slog.InfoContext(r.ctx, fmt.Sprintf("requeste vote response peerID: %d, peerTerm: %d, voteGranted: %t", peer.ID(), peerTerm, voteGranted))
		if peerTerm > currentTerm {
			slog.InfoContext(r.ctx, "peer term is bigger than current term, turning into follower")
			// TODO: set leader
			r.State = StateFollower
			return nil
		}
		if voteGranted {
			slog.InfoContext(r.ctx, "vote granted, setting peer voted for")
			peer.SetVotedFor(r.id)
		}
	}

	slog.InfoContext(r.ctx, "counting votes")
	wonElection, err := r.countVotes()
	if err != nil {
		return err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("voting results: %t", wonElection))
	if wonElection {
		slog.InfoContext(r.ctx, "won election, setting state to leader")
		r.State = StateLeader
	}

	return nil
}

// countVotes checks each peer for votes and if the candidate has a majority, returns true or an error
func (r *Raft) countVotes() (bool, error) {
	slog.InfoContext(r.ctx, "counting votes, setting initial votes to 1")
	totalVotes := 1
	votedFor, err := r.votedFor()
	if err != nil {
		return false, err
	}
	slog.InfoContext(r.ctx, fmt.Sprintf("found voted for: %d", votedFor))
	// convert to follower
	if votedFor != r.id {
		slog.InfoContext(r.ctx, "server did not vote for self, setting to -1 and converting to follower state")
		r.State = StateFollower
		if err := r.setVotedFor(-1); err != nil {
			return false, err
		}
		return false, nil
	}
	for _, peer := range r.peers {
		slog.InfoContext(r.ctx, fmt.Sprintf("peer %d voted for: %d", peer.ID(), peer.VotedFor()))
		if peer.VotedFor() == r.id {
			slog.InfoContext(r.ctx, "voted for server, incrementing total votes")
			totalVotes++
		}
	}
	majority := 1 + ((len(r.peers) + 1) / 2)
	slog.InfoContext(r.ctx, fmt.Sprintf("majority of votes needed is: %d", majority))

	if totalVotes < majority {
		slog.InfoContext(r.ctx, fmt.Sprintf("majority of votes is smaller than current total votes: totalVotes %d < majority %d", totalVotes, majority))
		return false, nil
	}
	slog.InfoContext(r.ctx, "majority of votes achieved, returning true for elected")
	return true, nil
}

// candidateState handles the logic for when a server turns to a candidate,
// the election timeout is reset and votes are requested from peers
func (r *Raft) candidateState() error {
	slog.InfoContext(r.ctx, "candidate state identified")
	slog.InfoContext(r.ctx, "locking mutex")
	r.mu.Lock()
	currentTerm, err := r.currentTerm()
	if err != nil {
		return fmt.Errorf("failed to get current term: %w", err)
	}
	currentTerm += 1
	slog.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	if err := r.setCurrentTerm(currentTerm); err != nil {
		return fmt.Errorf("failed to set current term: %w", err)
	}
	r.mu.Unlock()

	slog.InfoContext(r.ctx, "voting for itself")
	if err := r.setVotedFor(r.id); err != nil {
		return fmt.Errorf("failed to vote for self: %w", err)
	}

	r.resetElectionTimeout()
	slog.InfoContext(r.ctx, "requesting votes")
	if err := r.requestVotes(); err != nil {
		return err
	}
	return nil
}
