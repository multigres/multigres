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

# tool_checksums.sh - SHA256 checksums for known development tools
# This file contains verified checksums for binary downloads used in the project.
# To add a new tool/version, download the binary and calculate its SHA256 hash.

# Returns the SHA256 hash for a given tool/version/platform/arch combination
# Usage: get_sha256 <tool> <version> <platform> <arch> <extension>
get_sha256() {
  local tool="$1"
  local version="$2"
  local platform="$3"
  local arch="$4"
  local ext="$5"

  case "$tool" in
  "protoc")
    case "$version-$platform-$arch" in
    "25.1-linux-x86_64")
      echo "ed8fca87a11c888fed329d6a59c34c7d436165f662a2c875246ddb1ac2b6dd50"
      ;;
    "25.1-linux-aarch_64")
      echo "99975a8c11b83cd65c3e1151ae1714bf959abc0521acb659bf720524276ab0c8"
      ;;
    "25.1-osx-x86_64")
      echo "72c6d6b2bc855ff8688c3b7fb31288ccafd0ab55256ff8382d5711ecfcc11f4f"
      ;;
    "25.1-osx-aarch_64")
      echo "320308ce18c359564948754f51748de41cf02a4e7edf0cf47a805b9d38610f16"
      ;;
    *)
      echo "ERROR: no SHA256 hash available for protoc $version-$platform-$arch" >&2
      exit 1
      ;;
    esac
    ;;
  "etcd")
    case "$version-$platform-$arch-$ext" in
    # v3.6.4 checksums
    "v3.6.4-linux-amd64-tar.gz")
      echo "4d5f3101daa534e45ccaf3eec8d21c19b7222db377bcfd5e5a9144155238c105"
      ;;
    "v3.6.4-linux-arm64-tar.gz")
      echo "323421fa279f4f3d7da4c7f2dfa17d9e49529cb2b4cdf40899a7416bccdde42d"
      ;;
    "v3.6.4-darwin-amd64-zip")
      echo "3d66e809dc16f40a8fc0ec3809757ac0de42c18625afbdbd1e1b36bcabab7a77"
      ;;
    "v3.6.4-darwin-arm64-zip")
      echo "d2de8f331abcf22b7815551a7a915258e909277bcbb2b119eb47ed37ab6ae05d"
      ;;
    *)
      echo "ERROR: no SHA256 hash available for etcd $version-$platform-$arch.$ext" >&2
      exit 1
      ;;
    esac
    ;;
  *)
    echo "ERROR: no SHA256 hashes for tool $tool" >&2
    exit 1
    ;;
  esac
}
