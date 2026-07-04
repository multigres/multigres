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
SQLLOGICTEST_VER = v0.29.1
export SQLLOGICTEST_VER
# pgproto is built from source from the pgpool-II release tarball (it lives in
# the pgpool2 tree under src/tools/pgproto). PGPROTO_VER is the pgpool-II
# release version.
PGPROTO_VER = 4.6.6
export PGPROTO_VER

# List of all commands to build
CMDS = multigateway multipooler pgctld multiorch multigres multiadmin portpoolserver
BIN_DIR = bin

.PHONY: all build build-all clean images install test test-coverage pgregress pgregress-update-patches pgregress-update-patches-docker pgexternal pgexternal-update-patches pgproto pgproto-update-patches proto proto-ts tools parser metrics generate help

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

# Proto source files
PROTO_SRCS = $(shell find proto -name '*.proto')
PROTO_GO_OUTS = pb

# Proto source files for TypeScript generation (exclude google vendor protos)
PROTO_TS_SRCS = $(MTROOT)/proto/multiadminservice.proto $(MTROOT)/proto/clustermetadata.proto $(MTROOT)/proto/multigatewaymanagerdata.proto $(MTROOT)/proto/multipoolermanagerdata.proto
TS_PROTO_ES_PLUGIN = $(MTROOT)/web/multiadmin/node_modules/.bin/protoc-gen-es
TS_CONNECT_ES_PLUGIN = $(MTROOT)/web/multiadmin/node_modules/.bin/protoc-gen-connect-es
TS_PROTO_OUT = $(MTROOT)/web/multiadmin/lib/api/generated

# Generate protobuf files
proto: tools $(PROTO_GO_OUTS) proto-ts ## Generate protobuf files.

# Generate TypeScript types and connect service descriptors from proto files
proto-ts: $(PROTO_TS_SRCS) $(TS_PROTO_ES_PLUGIN)
	rm -rf $(TS_PROTO_OUT) && mkdir -p $(TS_PROTO_OUT)
	# import_extension=none omits the ".js" suffix protobuf-es adds to import paths
	# by default; Turbopack (used by Next.js) cannot resolve ".js" imports to ".ts"
	# files. The option is honored by all @bufbuild/protoplugin-based plugins.
	$(MTROOT)/dist/protoc-$(PROTOC_VER)/bin/protoc \
		--plugin=$(TS_PROTO_ES_PLUGIN) \
		--es_out=$(TS_PROTO_OUT) \
		--es_opt=target=ts,import_extension=none \
		--plugin=$(TS_CONNECT_ES_PLUGIN) \
		--connect-es_out=$(TS_PROTO_OUT) \
		--connect-es_opt=target=ts,import_extension=none \
		--proto_path=$(MTROOT)/proto \
		$(PROTO_TS_SRCS)

$(TS_PROTO_ES_PLUGIN): web/multiadmin/package.json
	cd $(MTROOT)/web/multiadmin && pnpm install

pb: $(PROTO_SRCS)
	$(MTROOT)/dist/protoc-$(PROTOC_VER)/bin/protoc \
	--plugin=$(MTROOT)/bin/protoc-gen-go --go_out=. \
	--go_opt=Mgoogle/api/annotations.proto=google.golang.org/genproto/googleapis/api/annotations \
	--go_opt=Mgoogle/api/http.proto=google.golang.org/genproto/googleapis/api/annotations \
	--plugin=$(MTROOT)/bin/protoc-gen-go-grpc --go-grpc_out=. \
	--plugin=$(MTROOT)/bin/protoc-gen-connect-go --connect-go_out=. \
	--connect-go_opt=module=github.com/multigres/multigres \
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
	go generate ./go/common/parser/...
	@echo "Parser and ast helpers generation completed"

generate: parser metrics ## Alias for parser and metrics catalog.

# Generate the metric catalog (go/observability/metriccatalog) from OpenTelemetry
# instrument definitions across the codebase.
metrics: ## Generate the Prometheus metric catalog/keep-list.
	@echo "$$(date): Generating metric catalog"
	go run ./go/tools/metricsgen/main
	@echo "Metric catalog generation completed"

##@ Build

# Build Go binaries only (debug, with symbols)
build: ## Build Go binaries (debug, with symbols).
	mkdir -p $(BIN_DIR)
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (debug)"; \
		go build -o $(BIN_DIR)/$$cmd ./go/cmd/$$cmd; \
	done

# Build Go binaries with coverage
build-coverage:
	mkdir -p bin/cov/
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (coverage)"; \
		go build -cover -covermode=atomic -coverpkg=./... -o $(BIN_DIR)/cov/$$cmd ./go/cmd/$$cmd; \
	done

# Build Go binaries only (release, static, stripped)
build-release: ## Build Go binaries (release, static, stripped).
	mkdir -p $(BIN_DIR)
	@for cmd in $(CMDS); do \
		echo "Building $$cmd (release)"; \
		CGO_ENABLED=0 go build -ldflags="-w -s" -o $(BIN_DIR)/$$cmd ./go/cmd/$$cmd; \
	done

# Build everything (proto + parser + binaries)
build-all: proto parser metrics build ## Build everything (proto + parser + metrics + binaries).

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

##@ Linting

naming-lint: ## Enforce single-word service names (Multipooler/Multiorch/Multigateway).
	./tools/naming_linter.sh

##@ Testing

# Run tests
test: pb build ## Run all tests.
	bin/portpoolserver --socket /tmp/multigres-port-pool.sock & \
	PORT_POOL_PID=$$!; \
	trap "kill $$PORT_POOL_PID" EXIT; \
	MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock go test ./...

test-short: ## Run short tests.
	go test -short -v ./...

test-race: ## Run tests with race detection.
	go test -short -v -race ./...

test-coverage: build-coverage ## Run tests with comprehensive coverage.
	bin/cov/portpoolserver --socket /tmp/multigres-port-pool.sock & \
	PORT_POOL_PID=$$!; \
	trap "kill $$PORT_POOL_PID" EXIT; \
	MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock ./scripts/go_test_coverage.sh ./...

# Run the PostgreSQL regression suite (patch-verify mode). Requires the test
# cluster to be built (via `make build`) and will clone/build PostgreSQL on
# first run.
pgregress: build ## Run the PostgreSQL regression suite with patch-based verification.
	RUN_PGREGRESS=1 PGREGRESS_PATCH_MODE=verify \
	go test -v -timeout 60m -run TestPostgreSQLRegression ./go/test/endtoend/pgregresstest/...

# Re-run the PostgreSQL regression suite in generate mode: any residual diff
# between actual output and patched-expected output is absorbed by (re)writing
# testdata/pg17/patches/<name>.patch. Review the resulting patches in the PR
# diff before merging.
pgregress-update-patches: build ## Regenerate testdata/pg17/patches/*.patch from the current run.
	RUN_PGREGRESS=1 PGREGRESS_PATCH_MODE=generate \
	go test -v -timeout 60m -run TestPostgreSQLRegression ./go/test/endtoend/pgregresstest/...

# Run the external extension suite (e.g. pgvector). Clones and builds each
# external extension as a PGXS module against the from-source PostgreSQL, then
# runs its shipped pg_regress suite through multigateway with patch-based
# verification. Known divergences are recorded under
# testdata/pg17/patches/external/<ext>/.
pgexternal: build ## Run the external extension suite (e.g. pgvector) with patch-based verification.
	RUN_PGEXTERNAL=1 PGREGRESS_PATCH_MODE=verify \
	go test -v -timeout 180m -run TestPostgreSQLRegression ./go/test/endtoend/pgregresstest/...

# Re-run the external extension suite in generate mode: any residual diff between
# actual output and patched-expected output is absorbed by (re)writing
# testdata/pg17/patches/external/<ext>/<name>.patch. Review the resulting patches
# in the PR diff before merging.
pgexternal-update-patches: build ## Regenerate testdata/pg17/patches/external/*.patch from the current run.
	RUN_PGEXTERNAL=1 PGREGRESS_PATCH_MODE=generate \
	go test -v -timeout 180m -run TestPostgreSQLRegression ./go/test/endtoend/pgregresstest/...

# Regenerate the FULL pgregress patch set (regression + isolation + contrib +
# external) inside an ubuntu-24.04 container that mirrors CI. The patch set is
# platform-sensitive (locale collation, timezone formatting, error-cursor
# positions), so it MUST be regenerated on Linux — running the targets above
# directly on macOS produces patches that fail CI verification. Patches are
# written back into the working tree via a bind mount. Requires Docker.
pgregress-update-patches-docker: ## Regenerate ALL pgregress patches in a CI-matching Linux container (Docker).
	./docker/pgregress-generate.sh

# Run the pgproto wire-protocol conformance suite (patch-verify mode). Requires
# `make build` and `make tools` (builds the pgproto binary); clones/builds
# PostgreSQL on first run. Known divergences are recorded as patches under
# go/test/endtoend/queryserving/pgproto/testdata/patches/.
pgproto: build ## Run the pgproto wire-protocol conformance suite with patch-based verification.
	RUN_EXTENDED_QUERY_SERVING_TESTS=1 PGPROTO_PATCH_MODE=verify \
	go test -v -timeout 30m -run TestPgProtoConformance ./go/test/endtoend/queryserving/pgproto/...

# Re-run the pgproto suite in generate mode: any residual divergence between the
# multigateway and PostgreSQL is absorbed by (re)writing
# go/test/endtoend/queryserving/pgproto/testdata/patches/<name>.patch. Review the
# resulting patches in the PR diff before merging.
pgproto-update-patches: build ## Regenerate pgproto testdata/patches/*.patch from the current run.
	RUN_EXTENDED_QUERY_SERVING_TESTS=1 PGPROTO_PATCH_MODE=generate \
	go test -v -timeout 30m -run TestPgProtoConformance ./go/test/endtoend/queryserving/pgproto/...
# Path to the supabase/postgres checkout. Override with SUPABASE_POSTGRES_DIR=... if needed.
SUPABASE_POSTGRES_DIR ?= $(HOME)/repos/supabase/postgres

docker-supabase-postgres: ## Build the supabase Postgres base image from Dockerfile-17.
	docker build \
	  -f $(SUPABASE_POSTGRES_DIR)/Dockerfile-17 \
	  -t supabase-postgres:local \
	  $(SUPABASE_POSTGRES_DIR)

docker-supabase-postgres-test: docker-supabase-postgres ## Build the integration test runner image (supabase Postgres + Go + etcd).
	docker build \
	  -f Dockerfile.integration-test \
	  --build-arg SUPABASE_IMAGE=supabase-postgres:local \
	  -t supabase-postgres-test:local \
	  .

test-integration-supabase: docker-supabase-postgres-test ## Run integration tests inside the supabase Postgres container.
	docker run --rm \
	  --name multigres-integration-test \
	  -v $(CURDIR):/multigres \
	  -v /tmp/go-cache:/home/postgres/.cache \
	  -w /multigres \
	  -e TEST_PRINT_LOGS=1 \
	  -e AWS_ACCESS_KEY_ID=test-access-key \
	  -e AWS_SECRET_ACCESS_KEY=test-secret-key \
	  supabase-postgres-test:local

##@ Maintenance

# Clean build artifacts
clean: ## Remove build artifacts and temp files.
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
