# Raft

This is an implementation of the Raft protocol described on [raft.github.io](raft.github.io)

The default transport is set as gRPC, but HTTP and internal mock transports are also available.

SQLite is used to persist the server state.
