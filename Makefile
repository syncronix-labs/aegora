APP_NAME=aegora

.PHONY: verify
verify: fmt tidy lint test build

.PHONY: all
all: fmt lint test build

.PHONY: fmt
fmt:
	go fmt ./...
	go vet ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -race -cover ./...

.PHONY: build
build:
	go build \
	-trimpath \
	-buildvcs=true \
	-ldflags="-s -w" \
	-o bin/$(APP_NAME) \
	./cmd/aegora

.PHONY: run
run:
	go run ./cmd/aegora

.PHONY: clean
clean:
	rm -rf bin

.PHONY: tidy
tidy:
	go mod tidy