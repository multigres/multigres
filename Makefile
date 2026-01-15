# Copyright 2025 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SHELL := /bin/bash

# These variables are used by the shell scripts.
MTROOT := $(shell pwd)
export MTROOT
PROTOC_VER = 25.1
export PROTOC_VER
ADDLICENSE_VER = v1.2.0
export ADDLICENSE_VER
ETCD_VER = v3.6.4
export ETCD_VER

# List of all commands to build
CMDS = multigateway multipooler pgctld multiorch multigres multiadmin
BIN_DIR = bin

.PHONY: all build build-all clean images install test test-coverage proto tools parser help

##@ General

# Default target
all: build ## Build all binaries (default).

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

# Install protobuf tools
tools: ## Install protobuf and build tools.
	echo $$(date): Installing build tools
	mkdir -p "$$(git rev-parse --git-dir)/hooks"
	ln -sf "$(MTROOT)/misc/git/pre-commit" "$$(git rev-parse --git-dir)/hooks/pre-commit"
	ln -sf "$(MTROOT)/misc/git/commit-msg" "$$(git rev-parse --git-dir)/hooks/commit-msg"
	./tools/setup_build_tools.sh
	go install golang.org/x/tools/cmd/goyacc@latest

# Proto source files
PROTO_SRCS = $(shell find proto -name '*.proto')
PROTO_GO_OUTS = pb

# Generate protobuf files
proto: tools $(PROTO_GO_OUTS) ## Generate protobuf files.

pb: $(PROTO_SRCS)
	$(MTROOT)/dist/protoc-$(PROTOC_VER)/bin/protoc \
	--plugin=$(MTROOT)/bin/protoc-gen-go --go_out=. \
	--go_opt=Mgoogle/api/annotations.proto=google.golang.org/genproto/googleapis/api/annotations \
	--go_opt=Mgoogle/api/http.proto=google.golang.org/genproto/googleapis/api/annotations \
	--plugin=$(MTROOT)/bin/protoc-gen-go-grpc --go-grpc_out=. \
	--plugin=$(MTROOT)/bin/protoc-gen-grpc-gateway --grpc-gateway_out=. \
	--grpc-gateway_opt=logtostderr=true \
	--grpc-gateway_opt=generate_unbound_methods=true \
		--proto_path=proto $(PROTO_SRCS) && \
	mkdir -p go/pb && \
	cp -Rf github.com/multigres/multigres/go/pb/* go/pb/ && \
	rm -rf github.com/ google.golang.org/

# Generate parser from grammar files
parser: ## Generate PostgreSQL parser from grammar.
	@echo "$$(date): Generating PostgreSQL parser from grammar and AST helpers"
	go generate ./go/parser/...
	@echo "Parser and ast helpers generation completed"

generate: parser ## Alias for parser.

##@ Build

# Build Go binaries only (debug, with symbols)
build: ## Build Go binaries (debug, with symbols).
	mkdir -p $(BIN_DIR)
	cp external/pico/pico.* go/common/web/templates/css/
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (debug)"; \
		go build -o $(BIN_DIR)/$$cmd ./go/cmd/$$cmd; \
	done

# Build Go binaries with coverage
build-coverage:
	mkdir -p bin/cov/
	cp external/pico/pico.* go/common/web/templates/css/
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (coverage)"; \
		go build -cover -covermode=atomic -coverpkg=./... -o $(BIN_DIR)/cov/$$cmd ./go/cmd/$$cmd; \
	done

# Build Go binaries only (release, static, stripped)
build-release: ## Build Go binaries (release, static, stripped).
	mkdir -p $(BIN_DIR)
	cp external/pico/pico.* go/common/web/templates/css/
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (release)"; \
		CGO_ENABLED=0 go build -ldflags="-w -s" -o $(BIN_DIR)/$$cmd ./go/cmd/$$cmd; \
	done

# Build everything (proto + parser + binaries)
build-all: proto parser build ## Build everything (proto + parser + binaries).

# TODO(sougou): images is a temporary convenience target for a demo.
# To run it, you need to have Docker installed.
# There is a kind cluster demo under the demo/k8s/ directory that uses these image tags.
images:
	docker build -t multigres/multigres:latest .
	docker build -f Dockerfile.pgctld -t multigres/pgctld-postgres:latest .
	docker build -t multigres/multiadmin-web:latest web/multiadmin

# Install binaries to GOPATH/bin
install: ## Install binaries to GOPATH/bin.
	@for cmd in $(CMDS); do \
		echo "Installing $$cmd"; \
		go install ./go/cmd/$$cmd; \
	done

##@ Testing

# Run tests
test: pb build ## Run all tests.
	go test ./...

test-short: ## Run short tests.
	go test -short -v ./...

test-race: ## Run tests with race detection.
	go test -short -v -race ./...

test-coverage: build-coverage ## Run tests with comprehensive coverage.
	./scripts/go_test_coverage.sh ./...

##@ Maintenance

# Clean build artifacts
clean: ## Remove build artifacts and temp files.
	rm -f go/common/web/templates/css/pico.*
	go clean -i ./go/...
	@for cmd in $(CMDS); do \
		echo "Removing $(BIN_DIR)/$$cmd"; \
		rm -f $(BIN_DIR)/$$cmd; \
	done

# Clean build and dependencies
clean-all: clean ## Remove build dependencies and distribution files.
	echo "Removing build dependencies..."
	rm -rf $(MTROOT)/dist $(MTROOT)/bin
	echo "Build dependencies removed. Run 'make tools' to reinstall."

validate-generated-files: clean build-all ## Validate that generated files match source.
	go mod tidy
	echo ""
	echo "Checking files modified during build..."
	MODIFIED_FILES=$$(git status --porcelain | grep "^ M" | awk '{print $$2}') ; \
	if [ -n "$$MODIFIED_FILES" ]; then \
		echo "Modified files found:"; \
		echo; \
		echo "$$MODIFIED_FILES"; \
		echo; \
		echo "Please run 'make build-all && go mod tidy' and commit the changes"; \
		exit 1; \
	else \
		echo "Generated files are up-to-date."; \
	fi
