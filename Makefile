VERSION=dev
TARGET=dist/raft
PROTO_DIR=proto
OUT_DIR=proto
TEST_COUNT?=20

default: build

download:
	go mod download

build-debug:
	go build -gcflags="-N -l" -o $(TARGET) .

build: download clean
	go build -o $(TARGET) -ldflags '-w -X main.Version=$(VERSION)' .

test: clean
	go test -v -race -timeout 180s ./...

e2e: clean
	go test -v -race -shuffle=on -count=$(TEST_COUNT) ./...

term-test: clean build
	./scripts/test.sh

.PHONY: proto
proto:
	protoc --proto_path=$(PROTO_DIR) \
		--go_out=$(OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto
	cd kv_client && \
	uv run -m grpc_tools.protoc -I../proto \
		--python_out=. --pyi_out=. \
		--grpc_python_out=. ../proto/keyval.proto

.PHONY: clean
clean:
	rm -rf dist && mkdir -p dist
