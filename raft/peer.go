package raft

import "sync"

type Peer struct {
	id       int
	address  string
	votedFor int
	mu       *sync.Mutex
}

func NewPeer(id int, address string) *Peer {
	return &Peer{
		id:       id,
		address:  address,
		votedFor: -1,
		mu:       &sync.Mutex{},
	}
}

func (p *Peer) ID() int {
	return p.id
}

func (p *Peer) Address() string {
	return p.address
}

func (p *Peer) SetVotedFor(votedFor int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.votedFor = votedFor
}

func (p *Peer) VotedFor() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.votedFor
}
