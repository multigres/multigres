# Copyright 2025 The Multigres Authors.
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

.PHONY: all build build-all clean install test proto tools clean_build_dep

# Default target
all: build

# Proto source files
PROTO_SRCS = $(shell find proto -name '*.proto')
PROTO_GO_OUTS = $(patsubst proto/%.proto,go/pb/%.pb.go,$(PROTO_SRCS))

# Install protobuf tools
tools:
	echo $$(date): Installing build tools
	./bash_tools/setup_build_tools.sh

# Generate protobuf files
proto: tools $(PROTO_GO_OUTS)

go/pb/%.pb.go: proto/%.proto
	mkdir -p go/pb
	. ./build.env && \
	$$MTROOT/dist/protoc-$$PROTOC_VER/bin/protoc --go_out=go/pb --go_opt=paths=source_relative \
		--go-grpc_out=go/pb --go-grpc_opt=paths=source_relative \
		--proto_path=proto $<

# Build Go binaries only
build:
	mkdir -p bin/
	go build -o bin/multigateway ./go/cmd/multigateway
	go build -o bin/multipooler ./go/cmd/multipooler
	go build -o bin/pgctld ./go/cmd/pgctld
	go build -o bin/multiorch ./go/cmd/multiorch

# Build everything (proto + binaries)
build-all: proto build

# Clean build artifacts
clean: clean_build_dep
	rm -rf bin/

# Install binaries to GOPATH/bin
install:
	go install ./go/cmd/multigateway
	go install ./go/cmd/multipooler
	go install ./go/cmd/pgctld
	go install ./go/cmd/multiorch

# Run tests
test:
	go test ./...

# Clean build dependencies
clean_build_dep:
	echo "Removing build dependencies..."
	. ./build.env && rm -rf $$MTROOT/dist
	echo "Build dependencies removed. Run 'make tools' to reinstall."
