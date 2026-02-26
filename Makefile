VERSION=dev
TARGET=dist/raft

default: build

download:
	go mod download

build-debug:
	go build -gcflags="-N -l" -o $(TARGET) .

build: download
	go build -o $(TARGET) -ldflags '-w -X main.Version=$(VERSION)' .

watch:
	air
