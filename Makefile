.PHONY: build clean install test

# Build all binaries
build:
	go build -o bin/multigateway ./cmd/multigateway
	go build -o bin/multipooler ./cmd/multipooler
	go build -o bin/pgctld ./cmd/pgctld
	go build -o bin/multiorch ./cmd/multiorch

# Clean build artifacts
clean:
	rm -rf bin/

# Install binaries to GOPATH/bin
install:
	go install ./cmd/multigateway
	go install ./cmd/multipooler
	go install ./cmd/pgctld
	go install ./cmd/multiorch

# Run tests
test:
	go test ./...