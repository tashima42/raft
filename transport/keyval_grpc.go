// Package transport encapsulates server transports
package transport

import (
	"context"
	"errors"

	"github.com/tashima42/raft/keyval"
	"github.com/tashima42/raft/proto"
)

type KeyValGRPCServer struct {
	proto.UnimplementedKeyValServer
	KeyVal *keyval.KeyVal
}

func (k *KeyValGRPCServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.Pack, error) {
	if req == nil {
		return nil, errors.New("invalid request")
	}
	value := k.KeyVal.Get(req.Key)
	return &proto.Pack{
		Key:   req.Key,
		Value: value,
	}, nil
}

func (k *KeyValGRPCServer) Set(ctx context.Context, req *proto.Pack) (*proto.Pack, error) {
	if req == nil {
		return nil, errors.New("invalid request")
	}
	pack := keyval.Pack{Key: req.Key, Value: req.Value}
	if err := k.KeyVal.SendLogToRaft(pack); err != nil {
		return nil, err
	}
	return req, nil
}
