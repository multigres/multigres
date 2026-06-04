#!/usr/bin/env bash
#
# Prepare JUnit results for the Trunk Flaky Tests uploader.
#
# Used by the composite action in ./action.yaml (the "Prepare JUnit results for
# Trunk" step) and runnable locally for debugging the conversion.
#
# Inputs (environment variables, mirroring the action's inputs):
#   JUNIT_PATHS   Comma-separated globs/paths to existing JUnit XML files.
#                 Takes precedence over JSON_FILE.
#   JSON_FILE     Path to a single `go test -json` JSONL file. Converted to
#                 JUnit XML when JUNIT_PATHS is empty.
#   PREV_OUTCOME  Outcome of the test step ("success" / "failure" / ...). An
#                 empty value is treated as "failure" so gating fails closed.
#
# Outputs:
#   In CI, appends `previous-step-outcome` and `junit-paths` to $GITHUB_OUTPUT.
#   Run locally (no $GITHUB_OUTPUT), it prints those values to stderr instead.
#
# Local examples:
#   JSON_FILE=short-test-results.jsonl ./prepare-junit.sh
#   JUNIT_PATHS=integration-test-results.xml ./prepare-junit.sh

set -euo pipefail

JUNIT_PATHS="${JUNIT_PATHS:-}"
JSON_FILE="${JSON_FILE:-}"
PREV_OUTCOME="${PREV_OUTCOME:-}"

# set_output NAME VALUE — append to $GITHUB_OUTPUT in CI; print to stderr when
# run locally so the resolved values are still visible.
set_output() {
  local name="$1" value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "${name}=${value}" >>"$GITHUB_OUTPUT"
  else
    echo "${name}=${value}" >&2
  fi
}

# Fail closed: an empty outcome (e.g. a stale step id that resolves to "") must
# not silently disable quarantine gating. Treat it as a failure so the uploader
# still gates on it.
set_output "previous-step-outcome" "${PREV_OUTCOME:-failure}"

if [[ -n "$JUNIT_PATHS" ]]; then
  # Forward the user's glob/list straight through to the uploader.
  out="$JUNIT_PATHS"
elif [[ -z "$JSON_FILE" ]]; then
  echo "::error::upload-test-results requires either junit-paths or json-file"
  exit 1
elif [[ -f "$JSON_FILE" ]]; then
  # go-junit-report converts the `go test -json` event stream into JUnit XML,
  # the format the Trunk uploader consumes. The module is checksum-verified
  # against the Go checksum database on download.
  go install github.com/jstemmer/go-junit-report/v2@v2.1.0
  out="${JSON_FILE%.*}.trunk-junit.xml"
  "$(go env GOPATH)/bin/go-junit-report" -parser gojson -in "$JSON_FILE" -out "$out"
else
  # Suite crashed before writing output; allow-missing-junit-files (the
  # uploader's default) keeps the missing path from being a hard error.
  echo "::warning::json-file '$JSON_FILE' not found; nothing to convert"
  out="${JSON_FILE%.*}.trunk-junit.xml"
fi

set_output "junit-paths" "$out"
