VERSION=dev
TARGET=dist/raft
PROTO_DIR = proto
OUT_DIR = proto


default: build

download:
	go mod download

build-debug:
	go build -gcflags="-N -l" -o $(TARGET) .

build: download
	go build -o $(TARGET) -ldflags '-w -X main.Version=$(VERSION)' .

test:
	go test -v -race ./...

watch:
	air

.PHONY: proto
proto:
	protoc --proto_path=$(PROTO_DIR) \
		--go_out=$(OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raft.proto

.PHONY: clean
clean:
	rm -f $(PROTO_DIR)/*.pb.go
