package raft

import "sync"

type Peer struct {
	id        int
	address   string
	kvAddress string
	votedFor  int
	nextIndex int
	mu        *sync.Mutex
}

func NewPeer(id int, address, kvAddress string) *Peer {
	return &Peer{
		id:        id,
		address:   address,
		kvAddress: kvAddress,
		votedFor:  -1,
		nextIndex: -1,
		mu:        &sync.Mutex{},
	}
}

func (p *Peer) ID() int {
	return p.id
}

func (p *Peer) Address() string {
	return p.address
}

func (p *Peer) KVAddress() string {
	return p.kvAddress
}

func (p *Peer) NextIndex() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextIndex
}

func (p *Peer) SetNextIndex(nextIndex int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextIndex = nextIndex
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
