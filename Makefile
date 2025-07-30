# Makefile for PostgreSQL Parser Port to Go
# Based on Vitess parser generation patterns from vitess/Makefile

.PHONY: all build test clean parser lint typecheck install-tools postgres-tests benchmark coverage

# Project configuration
PROJECT_NAME := multigres
PARSER_PKG := github.com/multigres/parser/go/parser
GO_MOD_ROOT := .

# Build settings
ifndef GOARCH
export GOARCH := $(shell go env GOARCH)
endif

ifndef GOOS
export GOOS := $(shell go env GOOS)
endif

ifndef GOPATH
export GOPATH := $(shell go env GOPATH)
endif

# PostgreSQL source directory for test integration
# Ported from postgres source integration requirements
POSTGRES_BASE_DIR := ../postgres

# Tools
GOYACC := goyacc

# Default target
all: build test

# Build the project
# Based on vitess/Makefile:88-99 build patterns
build: parser
	@echo "$$(date): Building multigres parser"
	go build -v ./go/...

# Install required tools for parser generation
# Following vitess tool installation patterns
install-tools:
	@echo "Installing parser generation tools..."
	go install golang.org/x/tools/cmd/goyacc@latest
	@echo "Tools installed successfully"

# Generate parser from grammar files
# Ported from vitess/Makefile:174-175 sqlparser generation
parser:
	@echo "$$(date): Generating PostgreSQL parser from grammar"
	go generate ./go/parser/...
	@echo "Parser generation completed"

# Validate that generated parser matches committed version
# Based on vitess CI validation patterns
parser-validate: parser
	@echo "Validating parser generation is reproducible"
	@if ! git diff --quiet go/parser/grammar/; then \
		echo "ERROR: Generated parser files differ from committed version"; \
		echo "Run 'make parser' and commit the changes"; \
		git diff go/parser/grammar/; \
		exit 1; \
	fi
	@echo "Parser validation passed"

# Run all tests
# Enhanced with PostgreSQL-specific test patterns
test: build
	@echo "$$(date): Running all tests"
	go test -v ./go/...

# Run tests with coverage
# Ported from vitess coverage patterns with parser exclusions
coverage:
	@echo "$$(date): Running tests with coverage"
	go test -coverprofile=coverage.out ./go/...
	# Remove generated parser files from coverage like vitess does
	# Based on vitess/Makefile:225-227 coverage cleanup
	sed -i'' -e '/^github\.com\/multigres\/parser\/go\/parser\/grammar\/.*\.go:/d' coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run benchmarks for parser performance
benchmark: build
	@echo "$$(date): Running parser benchmarks"
	# Keywords functionality is now integrated into lexer package
	go test -bench=. -benchmem ./go/parser/lexer/ 2>/dev/null || echo "Lexer benchmarks not yet available"
	go test -bench=. -benchmem ./go/parser/grammar/ 2>/dev/null || echo "Parser benchmarks not yet available"

# Run linting and static analysis
lint:
	@echo "$$(date): Running linting and static analysis"
	@which golangci-lint >/dev/null 2>&1 || { \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	}
	golangci-lint run ./go/...

# Run type checking
typecheck:
	@echo "$$(date): Running type checking"
	go vet ./go/...

# Run PostgreSQL compatibility tests
# Integration with PostgreSQL source regression tests
postgres-tests: build
	@echo "$$(date): Running PostgreSQL compatibility tests"
	@if [ ! -d "$(POSTGRES_BASE_DIR)" ]; then \
		echo "ERROR: PostgreSQL source directory not found at $(POSTGRES_BASE_DIR)"; \
		echo "Please ensure PostgreSQL source is available for test integration"; \
		exit 1; \
	fi
	go test -v -tags=postgres_integration ./go/internal/testutils/
	@echo "PostgreSQL compatibility tests completed"

# Clean build artifacts and generated files
clean:
	@echo "$$(date): Cleaning build artifacts"
	go clean ./go/...
	rm -f coverage.out coverage.html
	# Note: Keep generated parser files unless explicitly removing them
	# They should be committed to the repository for reproducible builds

# Development helper targets

# Quick test for development
dev-test: build
	@echo "$$(date): Running quick development tests"
	# Keywords functionality is now tested as part of lexer tests
	go test ./go/internal/testutils/

# Format Go code
fmt:
	@echo "$$(date): Formatting Go code"
	go fmt ./go/...

# Update Go modules
mod-tidy:
	@echo "$$(date): Tidying Go modules"
	go mod tidy

# Verify project setup
verify: install-tools parser-validate test lint typecheck
	@echo "$$(date): Project verification completed successfully"

# Development workflow target
dev: clean parser fmt dev-test
	@echo "$$(date): Development build completed"

# CI/CD workflow target  
ci: verify postgres-tests benchmark coverage
	@echo "$$(date): CI workflow completed successfully"

# Help target
help:
	@echo "PostgreSQL Parser Port - Makefile Help"
	@echo ""
	@echo "Main targets:"
	@echo "  all              - Build and test the project"
	@echo "  build            - Build the Go binaries"
	@echo "  test             - Run all tests"
	@echo "  parser           - Generate parser from grammar files"
	@echo "  clean            - Clean build artifacts"
	@echo ""
	@echo "Development targets:"
	@echo "  dev              - Quick development build and test"
	@echo "  dev-test         - Run quick tests for development"
	@echo "  fmt              - Format Go code"
	@echo "  lint             - Run linting and static analysis"
	@echo "  typecheck        - Run type checking"
	@echo ""
	@echo "Testing targets:"
	@echo "  postgres-tests   - Run PostgreSQL compatibility tests"
	@echo "  benchmark        - Run performance benchmarks"
	@echo "  coverage         - Generate test coverage report"
	@echo ""
	@echo "Tool targets:"
	@echo "  install-tools    - Install required parser generation tools"
	@echo "  parser-validate  - Validate parser generation is reproducible"
	@echo "  verify           - Run all verification checks"
	@echo "  ci               - Run full CI workflow"
	@echo ""
	@echo "Environment variables:"
	@echo "  POSTGRES_BASE_DIR - PostgreSQL source directory (default: ../postgres)"
	@echo "  GOARCH           - Target architecture (default: from go env)"
	@echo "  GOOS             - Target OS (default: from go env)"