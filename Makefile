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
PROTO_GO_OUTS = pb

# Install protobuf tools
tools:
	./tools/setup_build_tools.sh

# Generate protobuf files
proto: tools $(PROTO_GO_OUTS)

pb: $(PROTO_SRCS)
	. ./build.env && \
	$$MTROOT/dist/protoc-$$PROTOC_VER/bin/protoc --go_out=. \
		--go-grpc_out=. \
		--proto_path=proto $(PROTO_SRCS) && \
	mkdir -p go/pb && \
	cp -Rf github.com/multigres/multigres/go/pb/* go/pb/ && \
	rm -rf github.com/

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
clean:
	go clean -i ./go/...
	rm -f bin/*

# Install binaries to GOPATH/bin
install:
	go install ./go/cmd/multigateway
	go install ./go/cmd/multipooler
	go install ./go/cmd/pgctld
	go install ./go/cmd/multiorch

# Run tests
test:
	go test ./...

# Clean build and dependencies
clean_all: clean
	echo "Removing build dependencies..."
	. ./build.env && rm -rf $$MTROOT/dist $$MTROOT/bin
	echo "Build dependencies removed. Run 'make tools' to reinstall."
