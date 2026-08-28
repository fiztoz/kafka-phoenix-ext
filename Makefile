BIN      := bin/kafka-phoenix-ext
IMAGE    ?= ghcr.io/fiztoz/kafka-phoenix-ext
TAG      ?= dev

.PHONY: build run test vet fmt lint docker clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/kafka-phoenix-ext

run:
	CGO_ENABLED=0 go run ./cmd/kafka-phoenix-ext

test:
	CGO_ENABLED=0 go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

lint: vet
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed; go vet passed"

docker:
	docker build -t $(IMAGE):$(TAG) .

clean:
	rm -rf bin