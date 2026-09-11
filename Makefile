.PHONY: fmt vet test build verify docker-build

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...

build:
	mkdir -p bin
	go build -o bin/agent-runtime-foundry ./cmd/agent-runtime-foundry

verify: vet test build
	@test -z "$$(gofmt -l .)"

docker-build:
	docker build -t agent-runtime-foundry .
