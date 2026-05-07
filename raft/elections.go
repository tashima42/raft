package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	minimumElectionTimeoutMS int64 = 150
	maximumElectionTimeoutMS int64 = 2 * minimumElectionTimeoutMS
)

// resetElectionTimeout resets the election timeout to a random valuebetween
// the minimum and maximum election timeout values
func (r *Raft) resetElectionTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger.InfoContext(r.ctx, "resetting election timeout")
	r.electionTimeout = r.randomElectionTimeout()
	r.logger.InfoContext(r.ctx, fmt.Sprintf("new election timeout is: %s", r.electionTimeout.String()))
	r.electionResetTime = time.Now()
}

func (r *Raft) randomElectionTimeout() time.Duration {
	randTimeout := rand.Int64N(maximumElectionTimeoutMS - minimumElectionTimeoutMS)
	return time.Millisecond * time.Duration(minimumElectionTimeoutMS+randTimeout)
}

// electionTimer creates a ticker that checks if the election timeout has been reached
// and transitions the server to candidate state if it has.
func (r *Raft) electionTimer() {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.InfoContext(r.ctx, "cancel command received, closing.")
			return
		default:
			r.logger.InfoContext(r.ctx, "running election timer")

			electionTicker := time.NewTicker(10 * time.Millisecond).C

			for range electionTicker {
				r.mu.Lock()
				if r.State == StateLeader {
					r.logger.InfoContext(r.ctx, "in leader state, ignoring election tick")
					r.mu.Unlock()
					return
				}
				if time.Since(r.electionResetTime) >= r.electionTimeout {
					r.logger.InfoContext(r.ctx, "setting state to candidate and unlocking mutex")

					currentTerm, err := r.currentTerm()
					if err != nil {
						r.logger.ErrorContext(r.ctx, "candidate - failed to get current term: "+err.Error())
						return
					}
					currentTerm += 1
					r.logger.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
					if err := r.setCurrentTerm(currentTerm); err != nil {
						r.logger.ErrorContext(r.ctx, "failed to set current term: "+err.Error())
						r.mu.Unlock()
						return
					}
					r.State = StateCandidate
					r.mu.Unlock()
					return
				}
				r.mu.Unlock()
			}
		}
	}
}

// requestVotes sends a RequestVote RPC to all peers and updates the server's state based
// on the results. If the server has achieved a majority of votes, it becomes the lader.
// If the server founds a peer with a higher term, it steps down to follower and updates
// its term to the higher term.
func (r *Raft) requestVotes() error {
	r.logger.InfoContext(r.ctx, "requesting votes, locking mutex")
	currentTerm, err := r.currentTerm()
	if err != nil {
		return err
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("current term: %d", currentTerm))
	// TODO: issue parallel vote requests and use channels for sync
	wg := &sync.WaitGroup{}
	electionCtx, electionCancel := context.WithCancel(r.ctx)
	defer electionCancel()
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer *Peer) {
			defer wg.Done()
			r.logger.InfoContext(electionCtx, "requesting vote from: "+peer.Address())
			// TODO: implement last log
			tCtx, tCancel := context.WithTimeout(electionCtx, time.Second*1)
			defer tCancel()
			peerTerm, voteGranted, err := r.Client.RequestVote(tCtx, *peer, RequestVoteRequest{Term: currentTerm, CandidateID: r.id, LastLogIndex: 0, LastLogTerm: 0})
			if err != nil {
				r.logger.ErrorContext(r.ctx, "failed to get response from request vote: "+err.Error())
				peerTerm = -1
				voteGranted = false
			}
			r.logger.InfoContext(r.ctx, fmt.Sprintf("peerID: %d, peerTerm: %d, voteGranted: %t", peer.ID(), peerTerm, voteGranted))
			if peerTerm > currentTerm {
				r.logger.InfoContext(r.ctx, "peer term is bigger than current term, turning into follower")
				// TODO: set leader
				r.mu.Lock()
				r.State = StateFollower
				r.mu.Unlock()
				electionCancel()
				return
			}
			if voteGranted {
				r.logger.InfoContext(r.ctx, "vote granted, setting peer voted for")
				peer.SetVotedFor(r.id)
			}
		}(peer)
	}
	wg.Wait()

	r.logger.InfoContext(r.ctx, "counting votes")
	wonElection, err := r.countVotes()
	if err != nil {
		return err
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("voting results: %t", wonElection))
	if wonElection {
		r.logger.InfoContext(r.ctx, "won election, setting state to leader")
		r.mu.Lock()
		r.State = StateLeader
		r.mu.Unlock()
	}
	return nil
}

// countVotes checks each peer for votes and if the candidate has a majority, returns true or an error
func (r *Raft) countVotes() (bool, error) {
	r.logger.InfoContext(r.ctx, "counting votes")
	totalVotes := 1
	votedFor, err := r.votedFor()
	if err != nil {
		return false, err
	}
	// convert to follower
	if votedFor != r.id {
		r.logger.InfoContext(r.ctx, "server did not vote for self, setting to -1 and converting to follower state")

		r.mu.Lock()
		r.State = StateFollower
		r.mu.Unlock()
		if err := r.setVotedFor(-1); err != nil {
			return false, err
		}
		return false, nil
	}
	for _, peer := range r.peers {
		r.logger.InfoContext(r.ctx, fmt.Sprintf("peer %d voted for: %d", peer.ID(), peer.VotedFor()))
		if peer.VotedFor() == r.id {
			totalVotes++
		}
	}
	majority := 1 + ((len(r.peers) + 1) / 2)

	if totalVotes < majority {
		r.logger.InfoContext(r.ctx, fmt.Sprintf("majority of votes is smaller than current total votes: totalVotes %d < majority %d", totalVotes, majority))
		return false, nil
	}
	r.logger.InfoContext(r.ctx, fmt.Sprintf("total votes: %d >= %d, returning true for elected", totalVotes, majority))
	return true, nil
}

// candidateState handles the logic for when a server turns to a candidate,
// the election timeout is reset and votes are requested from peers
func (r *Raft) candidateState() error {
	for {
		select {
		case <-r.ctx.Done():
			return nil
		default:
			r.logger.InfoContext(r.ctx, "candidate state identified")

			r.logger.InfoContext(r.ctx, "voting for itself")
			if err := r.setVotedFor(r.id); err != nil {
				return fmt.Errorf("failed to vote for self: %w", err)
			}

			r.resetElectionTimeout()
			r.logger.InfoContext(r.ctx, "requesting votes")
			if err := r.requestVotes(); err != nil {
				return err
			}
			return nil
		}
	}
}
