VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build linux test lint local tidy clean

build:            ## build for this machine
	go build -trimpath -ldflags "$(LDFLAGS)" -o tunnelbench ./cmd/tunnelbench

linux:            ## build static Linux binaries to copy to servers with scp
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/tunnelbench_linux_amd64 ./cmd/tunnelbench
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/tunnelbench_linux_arm64 ./cmd/tunnelbench

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run

local: build      ## quick self-test over loopback
	./tunnelbench local --quick --listen 127.0.0.1:23000

tidy:
	go mod tidy

clean:
	rm -rf tunnelbench dist tunnelbench-runs
