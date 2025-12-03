#!/bin/bash
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

# safe_download - A robust wrapper around curl for downloading files
# Provides clear error messages when downloads fail due to HTTP errors
# Verifies SHA256 checksums for supply chain security
# Includes retry logic for transient network failures

set -euo pipefail

# Usage: safe_download <url> <output_file> <sha256>
# Downloads a file from URL, saves it to output_file, and verifies its SHA256 checksum
# Exits with code 1 and a clear error message if the download or verification fails
safe_download() {
  local url="$1"
  local output_file="$2"
  local expected_sha256="$3"

  if [ -z "$url" ] || [ -z "$output_file" ] || [ -z "$expected_sha256" ]; then
    echo "❌ Error: safe_download requires URL, output file, and SHA256 hash" >&2
    echo "Usage: safe_download <url> <output_file> <sha256>" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  safe_download https://example.com/file.tar.gz file.tar.gz abc123def456..." >&2
    return 1
  fi

  # Validate SHA256 format (64 hex characters)
  if ! [[ "$expected_sha256" =~ ^[a-fA-F0-9]{64}$ ]]; then
    echo "❌ Error: Invalid SHA256 hash format" >&2
    echo "Expected: 64 hexadecimal characters" >&2
    echo "Got: $expected_sha256" >&2
    return 1
  fi

  # Retry logic: attempt download up to 3 times with exponential backoff
  local max_attempts=3
  local attempt=1
  local download_success=false

  while [ $attempt -le $max_attempts ]; do
    if [ $attempt -gt 1 ]; then
      local wait_time=$((2 ** (attempt - 1)))
      echo "⏳ Retrying in $wait_time seconds... (attempt $attempt/$max_attempts)" >&2
      sleep "$wait_time"
    fi

    echo "📥 Downloading from $url (attempt $attempt/$max_attempts)..." >&2

    # Attempt download with proper error handling
    # -f: Fail silently on HTTP errors (4xx, 5xx) - returns exit code 22
    # -S: Show errors even in silent mode
    # -L: Follow redirects
    # --connect-timeout: Fail if connection takes longer than this
    # --max-time: Fail if entire operation takes longer than this
    if curl -fsSL \
      --connect-timeout 30 \
      --max-time 300 \
      -o "$output_file" \
      "$url"; then
      download_success=true
      break
    fi

    local exit_code=$?
    attempt=$((attempt + 1))

    # On last attempt, provide detailed error information
    if [ $attempt -gt $max_attempts ]; then
      echo "" >&2
      echo "❌ Error: Failed to download file after $max_attempts attempts" >&2
      echo "URL: $url" >&2
      echo "" >&2

      # Provide specific guidance based on curl exit code
      case $exit_code in
      6)
        echo "Cause: Could not resolve host" >&2
        echo "  - Check your internet connection" >&2
        echo "  - Verify the URL is correct" >&2
        ;;
      7)
        echo "Cause: Failed to connect to host" >&2
        echo "  - Check your internet connection" >&2
        echo "  - Check if a firewall is blocking the connection" >&2
        ;;
      22)
        echo "Cause: HTTP error (4xx or 5xx status code)" >&2
        echo "  - The file may have been moved or deleted" >&2
        echo "  - You may have hit a rate limit" >&2
        echo "  - Check the URL in a browser: $url" >&2
        ;;
      28)
        echo "Cause: Operation timed out" >&2
        echo "  - Check your internet connection" >&2
        echo "  - The server may be slow or unresponsive" >&2
        ;;
      *)
        echo "Cause: curl exited with code $exit_code" >&2
        echo "  - See 'man curl' for details about exit code $exit_code" >&2
        ;;
      esac

      # Clean up partial download
      rm -f "$output_file"
      return 1
    fi
  done

  if [ "$download_success" = false ]; then
    rm -f "$output_file"
    return 1
  fi

  # Verify the file was actually written and has content
  if [ ! -f "$output_file" ] || [ ! -s "$output_file" ]; then
    echo "❌ Error: Download completed but output file is missing or empty" >&2
    echo "URL: $url" >&2
    echo "Output: $output_file" >&2
    rm -f "$output_file"
    return 1
  fi

  # Verify SHA256 checksum
  echo "🔐 Verifying SHA256 checksum..." >&2
  local actual_sha256
  actual_sha256=$(shasum -a 256 "$output_file" | awk '{print $1}')

  if [ "$actual_sha256" != "$expected_sha256" ]; then
    echo "" >&2
    echo "❌ Error: SHA256 checksum verification failed!" >&2
    echo "This indicates the downloaded file may have been tampered with or corrupted." >&2
    echo "" >&2
    echo "Expected: $expected_sha256" >&2
    echo "Got:      $actual_sha256" >&2
    echo "URL:      $url" >&2
    echo "" >&2
    echo "DO NOT USE THIS FILE. It may be malicious or corrupted." >&2

    # Clean up the potentially compromised file
    rm -f "$output_file"
    return 1
  fi

  echo "✅ Download complete and verified" >&2
  return 0
}

# If script is executed directly (not sourced), run the function with arguments
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  safe_download "$@"
fi
