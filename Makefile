.PHONY: build test vet lint bench run verify

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

# Requires: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
lint:
	golangci-lint run

bench:
	go test -bench=. -benchmem ./bench/...

run: build
	go run ./cmd/firnd

verify: vet test
	@echo "verify OK"
