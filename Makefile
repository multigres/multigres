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