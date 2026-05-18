package transport

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// leaderChecker is satisfied by *raft.Raft, allowing the interceptor to avoid
// a direct import cycle between transport and raft packages.
type leaderChecker interface {
	IsLeader() bool
	LeaderKVAddress() (string, error)
}

// LeaderRedirectInterceptor returns a UnaryServerInterceptor that redirects
// requests to the current Raft leader when this node is not the leader.
// The leader's KV address is sent back in the "redirect-to" trailing metadata key
// alongside a codes.Unavailable status, so clients can reconnect and retry.
func LeaderRedirectInterceptor(r leaderChecker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if r.IsLeader() {
			return handler(ctx, req)
		}

		leaderAddr, err := r.LeaderKVAddress()
		if err != nil || leaderAddr == "" {
			if err != nil {
				log.Printf("leader redirect failed to resolve leader address: %v", err)
			} else {
				log.Printf("leader redirect failed: leader address is empty")
			}
			return nil, status.Error(codes.Unavailable, "not the leader and leader address is unknown")
		}

		grpc.SetTrailer(ctx, metadata.Pairs("redirect-to", leaderAddr))
		return nil, status.Errorf(codes.Unavailable, "not the leader, redirect to %s", leaderAddr)
	}
}
