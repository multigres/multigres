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

# download_tool.sh - Convenience wrapper for downloading known development tools
# Usage: download_tool.sh <tool> <version> <platform-arch> <output_file>
# Example: download_tool.sh etcd v3.5.0 linux-amd64 etcd.tar.gz

set -euo pipefail

# Source the checksum registry
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/tool_checksums.sh
source "$SCRIPT_DIR/tool_checksums.sh"
# shellcheck source=tools/safe_download.sh
source "$SCRIPT_DIR/safe_download.sh"

download_tool() {
  local tool="$1"
  local version="$2"
  local platform_arch="$3"
  local output_file="$4"

  if [ -z "$tool" ] || [ -z "$version" ] || [ -z "$platform_arch" ] || [ -z "$output_file" ]; then
    echo "❌ Error: download_tool requires tool, version, platform-arch, and output file" >&2
    echo "Usage: download_tool <tool> <version> <platform-arch> <output_file>" >&2
    echo "" >&2
    echo "Examples:" >&2
    echo "  download_tool etcd v3.5.0 linux-amd64 etcd.tar.gz" >&2
    echo "  download_tool protoc 25.1 osx-aarch_64 protoc.zip" >&2
    return 1
  fi

  # Parse platform-arch (e.g., "linux-amd64" -> platform="linux", arch="amd64")
  local platform="${platform_arch%%-*}"
  local arch="${platform_arch##*-}"

  # Determine file extension based on platform
  local ext
  case "$tool-$platform" in
  etcd-linux)
    ext="tar.gz"
    ;;
  etcd-darwin)
    ext="zip"
    ;;
  protoc-*)
    ext="zip"
    ;;
  *)
    echo "❌ Error: unknown tool/platform combination: $tool-$platform" >&2
    return 1
    ;;
  esac

  # Construct download URL based on tool
  local url
  case "$tool" in
  etcd)
    local filename="etcd-${version}-${platform}-${arch}.${ext}"
    url="https://github.com/etcd-io/etcd/releases/download/${version}/${filename}"
    ;;
  protoc)
    # protoc uses x86_64/aarch_64 naming, and "osx" instead of "darwin"
    local protoc_platform="$platform"
    if [ "$platform" = "darwin" ]; then
      protoc_platform="osx"
    fi
    local protoc_arch="$arch"
    if [ "$arch" = "amd64" ]; then
      protoc_arch="x86_64"
    elif [ "$arch" = "arm64" ]; then
      protoc_arch="aarch_64"
    fi
    local filename="protoc-${version}-${protoc_platform}-${protoc_arch}.${ext}"
    url="https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${filename}"
    ;;
  *)
    echo "❌ Error: unknown tool: $tool" >&2
    echo "Supported tools: etcd, protoc" >&2
    return 1
    ;;
  esac

  # Get SHA256 hash
  local sha256
  if [ "$tool" = "protoc" ]; then
    # protoc uses different arch naming in checksums
    local protoc_platform_for_hash="$platform"
    if [ "$platform" = "darwin" ]; then
      protoc_platform_for_hash="osx"
    fi
    local protoc_arch_for_hash="$arch"
    if [ "$arch" = "amd64" ]; then
      protoc_arch_for_hash="x86_64"
    elif [ "$arch" = "arm64" ]; then
      protoc_arch_for_hash="aarch_64"
    fi
    sha256=$(get_sha256 "$tool" "$version" "$protoc_platform_for_hash" "$protoc_arch_for_hash" "$ext")
  else
    sha256=$(get_sha256 "$tool" "$version" "$platform" "$arch" "$ext")
  fi

  # Download with verification
  safe_download "$url" "$output_file" "$sha256"
}

# If script is executed directly (not sourced), run the function with arguments
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  download_tool "$@"
fi
