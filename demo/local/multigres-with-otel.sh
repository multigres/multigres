#!/usr/bin/env bash
# Copyright 2026 Supabase, Inc.
# Wrapper script to run multigres cluster commands with OTEL observability

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"
export PATH="$REPO_ROOT/bin:$PATH"
exec "$SCRIPT_DIR/run-with-otel.sh" go run ./go/cmd/multigres "$@"
