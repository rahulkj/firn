.PHONY: build test vet lint bench run verify

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

# Requires: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
lint:
	golangci-lint run

bench:
	go test -bench=. -benchmem ./bench/...

run: build
	go run ./cmd/eventingd

verify: vet test
	@echo "verify OK"
