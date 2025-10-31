# Copyright 2025 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is a distributed on an "AS IS" BASIS,
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

CMDS = multigateway multipooler pgctld multiorch multigres multiadmin
OUTPUT_DIR = bin

.PHONY: all build build-release build-all clean install test proto tools parser

# Default target
all: build

# Proto source files
PROTO_SRCS = $(shell find proto -name '*.proto')
PROTO_GO_OUTS = pb

# Install protobuf tools
tools:
	echo $$(date): Installing build tools
	mkdir -p .git/hooks
	ln -sf "$(MTROOT)/misc/git/pre-commit" .git/hooks/pre-commit
	ln -sf "$(MTROOT)/misc/git/commit-msg" .git/hooks/commit-msg
	./tools/setup_build_tools.sh
	go install golang.org/x/tools/cmd/goyacc@latest

# Generate protobuf files
proto: tools $(PROTO_GO_OUTS)

pb: $(PROTO_SRCS)
	$(MTROOT)/dist/protoc-$(PROTOC_VER)/bin/protoc \
	--plugin=$(MTROOT)/bin/protoc-gen-go --go_out=. \
	--plugin=$(MTROOT)/bin/protoc-gen-go-grpc --go-grpc_out=. \
		--proto_path=proto $(PROTO_SRCS) && \
	mkdir -p go/pb && \
	cp -Rf github.com/multigres/multigres/go/pb/* go/pb/ && \
	rm -rf github.com/

# Generate parser from grammar files
# Ported from vitess/Makefile:174-175 sqlparser generation
parser:
	@echo "$$(date): Generating PostgreSQL parser from grammar"
	go generate ./go/parser/...
	@echo "Parser generation completed"

# Build Go binaries only (debug, with symbols)
build:
	mkdir -p $(OUTPUT_DIR)
	cp external/pico/pico.* go/web/templates/css/
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (debug)"; \
		go build -o $(OUTPUT_DIR)/$$cmd ./go/cmd/$$cmd; \
	done

# Build Go binaries only (release, static, stripped)
build-release:
	mkdir -p $(OUTPUT_DIR)
	cp external/pico/pico.* go/web/templates/css/
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (release)"; \
		CGO_ENABLED=0 go build -ldflags="-w -s" -o $(OUTPUT_DIR)/$$cmd ./go/cmd/$$cmd; \
	done

# Build everything (proto + parser + binaries)
build-all: proto parser build

# Clean build artifacts
clean:
	rm -f go/web/templates/css/pico.*
	go clean -i ./go/...
	rm -f $(OUTPUT_DIR)/*

# Install binaries to GOPATH/bin
install:
	@for cmd in $(CMDS); do \
		go install ./go/cmd/$$cmd; \
	done

# Run tests
test: pb build
	go test ./...

test-short:
	go test -short -v ./...

# Clean build and dependencies
clean-all: clean
	echo "Removing build dependencies..."
	rm -rf $(MTROOT)/dist $(MTROOT)/bin
	echo "Build dependencies removed. Run 'make tools' to reinstall."

validate-generated-files: clean build-all
	echo ""
	echo "Checking files modified during build..."
	MODIFIED_FILES=$$(git status --porcelain | grep "^ M" | awk '{print $$2}') ; \
	if [ -n "$$MODIFIED_FILES" ]; then \
		echo "Modified files found:"; \
		echo; \
		echo "$$MODIFIED_FILES"; \
		echo; \
		echo "Please run 'make build-all' and commit the changes"; \
		exit 1; \
	else \
		echo "Generated files are up-to-date.";