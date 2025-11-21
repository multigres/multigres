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

set -euo pipefail

# Source helper scripts
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/safe_download.sh
source "$SCRIPT_DIR/safe_download.sh"
# shellcheck source=tools/tool_checksums.sh
source "$SCRIPT_DIR/tool_checksums.sh"

# Multigres build tools setup script
# Installs build dependencies using install_dep pattern

# Dependency versions
PROTOC_VERSION="$PROTOC_VER"
ADDLICENSE_VERSION="$ADDLICENSE_VER"
ETCD_VERSION="$ETCD_VER"
PGBACKREST_VERSION="${PGBACKREST_VER:-2.57.0}"

get_platform() {
  case $(uname) in
  Linux) echo "linux" ;;
  Darwin) echo "osx" ;;
  *)
    echo "ERROR: unsupported platform"
    exit 1
    ;;
  esac
}

get_arch() {
  case $(uname -m) in
  x86_64) echo "x86_64" ;;
  aarch64) echo "aarch_64" ;;
  arm64)
    case $(get_platform) in
    osx) echo "aarch_64" ;;
    *)
      echo "ERROR: unsupported architecture"
      exit 1
      ;;
    esac
    ;;
  *)
    echo "ERROR: unsupported architecture"
    exit 1
    ;;
  esac
}

install_dep() {
  local name="$1"
  local version="$2"
  local dist="$3"

  local version_file="$dist/.installed_version"

  # Check if already installed with correct version
  if [[ -f "$version_file" && "$(cat "$version_file")" == "$version" ]]; then
    return 0
  fi

  echo "Installing $name $version..."

  # Clean up any existing installation
  rm -rf "$dist"
  mkdir -p "$dist"

  case "$name" in
  "protoc")
    install_protoc_impl "$version" "$dist"
    ;;
  "etcd")
    install_etcd "$version" "$dist"
    ;;
  "pgbackrest")
    install_pgbackrest "$version" "$dist"
    ;;
  *)
    echo "ERROR: unknown dependency $name"
    exit 1
    ;;
  esac

  # Mark as installed with this version
  echo "$version" >"$version_file"
  echo "$name installed successfully"
}

install_protoc_impl() {
  local version="$1"
  local dist="$2"

  local platform
  platform=$(get_platform)
  local arch
  arch=$(get_arch)
  local filename="protoc-${version}-${platform}-${arch}.zip"
  local url="https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${filename}"

  # Get SHA256 hash for verification
  local sha256
  sha256=$(get_sha256 "protoc" "$version" "$platform" "$arch" "zip")

  cd "$dist"

  echo "Downloading ${url}..."
  safe_download "${url}" "${filename}" "${sha256}"

  echo "Extracting protoc..."
  unzip -q "${filename}"

  # protoc is now available at $dist/bin/protoc

  # Clean up
  rm "${filename}"
  cd - >/dev/null
}

# Download and install etcd, link etcd binary into our root.
install_etcd() {
  local version="$1"
  local dist="$2"

  case $(uname) in
  Linux)
    local platform=linux
    local ext=tar.gz
    ;;
  Darwin)
    local platform=darwin
    local ext=zip
    ;;
  *)
    echo "ERROR: unsupported platform for etcd"
    exit 1
    ;;
  esac

  case $(uname -m) in
  aarch64) local arch=arm64 ;;
  x86_64) local arch=amd64 ;;
  arm64) local arch=arm64 ;;
  *)
    echo "ERROR: unsupported architecture for etcd"
    exit 1
    ;;
  esac

  local filename="etcd-${version}-${platform}-${arch}.${ext}"

  # This is how we'd download directly from source:
  local url="https://github.com/etcd-io/etcd/releases/download/$version/$filename"

  # Get SHA256 hash for verification
  local sha256
  sha256=$(get_sha256 "etcd" "$version" "$platform" "$arch" "$ext")

  cd "$dist"
  echo "Downloading ${url}..."
  safe_download "${url}" "${filename}" "${sha256}"

  echo "Extracting etcd..."

  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$filename"
  else
    unzip -q "$filename"
  fi

  rm "$filename"
  mkdir -p "$MTROOT/bin"
  ln -snf "$dist/etcd-${version}-${platform}-${arch}/etcd" "$MTROOT/bin/etcd"
  ln -snf "$dist/etcd-${version}-${platform}-${arch}/etcdctl" "$MTROOT/bin/etcdctl"
  cd - >/dev/null
}

# Compare versions (returns 0 if installed >= required, 1 otherwise)
version_compare() {
  local installed="$1"
  local required="$2"

  # Remove 'v' prefix if present
  installed="${installed#v}"
  required="${required#v}"

  # Split versions into arrays
  IFS='.' read -ra installed_parts <<<"$installed"
  IFS='.' read -ra required_parts <<<"$required"

  # Compare each part
  for i in 0 1 2; do
    local installed_part="${installed_parts[$i]:-0}"
    local required_part="${required_parts[$i]:-0}"

    if [ "$installed_part" -gt "$required_part" ]; then
      return 0
    elif [ "$installed_part" -lt "$required_part" ]; then
      return 1
    fi
  done

  return 0
}

# Use the appropriate method to install pgBackRest based on the platform.
install_pgbackrest() {
  local version="$1"
  local dist="$2"

  local platform
  platform=$(get_platform)

  case "$platform" in
  linux)
    install_pgbackrest_linux "$version" "$dist"
    ;;
  osx)
    install_pgbackrest_macos "$version" "$dist"
    ;;
  *)
    echo "ERROR: unsupported platform for pgBackRest"
    exit 1
    ;;
  esac

  # Verify installation succeeded
  if [ ! -f "$MTROOT/bin/pgbackrest" ]; then
    echo "ERROR: pgBackRest installation failed"
    exit 1
  fi

  # Display and verify installed version
  local installed_version
  installed_version=$("$MTROOT/bin/pgbackrest" version | head -n1 | awk '{print $2}')

  if ! version_compare "$installed_version" "$version"; then
    echo "WARNING: pgBackRest version ${installed_version} is older than recommended version ${version}"
    echo "Consider upgrading pgBackRest to at least version ${version}"
  fi

  echo "pgBackRest ${installed_version} is now available"
}

# If there's already a pgbackrest binary in the system, use it. Otherwise,
# install it via package manager.
install_pgbackrest_linux() {
  local version="$1"
  local dist="$2"

  mkdir -p "$MTROOT/bin"

  # First, check if already installed on system
  if command -v pgbackrest >/dev/null 2>&1; then
    echo "pgBackRest found on system, checking version..."
    local installed_version
    installed_version=$(pgbackrest version | head -n1 | awk '{print $2}')

    if version_compare "$installed_version" "$version"; then
      echo "System pgBackRest ${installed_version} meets requirement (>= ${version}), creating symlink..."
      ln -snf "$(command -v pgbackrest)" "$MTROOT/bin/pgbackrest"
      return 0
    else
      echo "System pgBackRest ${installed_version} is older than required ${version}"
      echo "Will attempt to install/upgrade via package manager..."
    fi
  fi

  # Try to detect package manager and install
  echo "Attempting to install pgBackRest via package manager..."

  if command -v apt-get >/dev/null 2>&1; then
    # Debian/Ubuntu - use PostgreSQL apt repository for latest version
    echo "Detected apt package manager"
    echo "Setting up PostgreSQL apt repository for latest pgBackRest..."

    # Install prerequisites for PostgreSQL apt repository
    sudo apt-get install -y curl ca-certificates 2>/dev/null || true

    # Check if PostgreSQL repo is already configured
    if [ ! -f /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc ]; then
      # Install the repository key
      sudo apt install -y postgresql-common ca-certificates
      sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
      sudo apt update
    else
      echo "PostgreSQL apt repository already configured"
    fi

    # Update and install
    if sudo apt-get update && sudo apt-get install -y pgbackrest; then
      ln -snf "$(command -v pgbackrest)" "$MTROOT/bin/pgbackrest"
      return 0
    fi
  elif command -v yum >/dev/null 2>&1; then
    # RHEL/CentOS/Fedora
    echo "Detected yum package manager"
    if sudo yum install -y pgbackrest; then
      ln -snf "$(command -v pgbackrest)" "$MTROOT/bin/pgbackrest"
      return 0
    fi
  elif command -v dnf >/dev/null 2>&1; then
    # Fedora/RHEL 8+
    echo "Detected dnf package manager"
    if sudo dnf install -y pgbackrest; then
      ln -snf "$(command -v pgbackrest)" "$MTROOT/bin/pgbackrest"
      return 0
    fi
  elif command -v zypper >/dev/null 2>&1; then
    # openSUSE
    echo "Detected zypper package manager"
    if sudo zypper install -y pgbackrest; then
      ln -snf "$(command -v pgbackrest)" "$MTROOT/bin/pgbackrest"
      return 0
    fi
  fi

  # If we get here, package manager installation failed or wasn't available
  echo "ERROR: Could not install pgBackRest automatically"
  echo ""
  echo "Please install pgBackRest manually using one of these methods:"
  echo ""
  echo "Debian/Ubuntu (using PostgreSQL apt repository):"
  echo "  sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh"
  echo "  sudo apt-get update"
  echo "  sudo apt-get install pgbackrest"
  echo ""
  echo "RHEL/CentOS/Fedora:"
  echo "  sudo dnf install pgbackrest"
  echo "  # or: sudo yum install pgbackrest"
  echo ""
  echo "From source:"
  echo "  # Install dependencies first:"
  echo "  sudo apt-get install libpq-dev libxml2-dev libssl-dev zlib1g-dev"
  echo "  # Then download and build from: https://github.com/pgbackrest/pgbackrest/releases"
  echo ""
  exit 1
}

# If there's already a pgbackrest binary in the system, use it. Otherwise,
# install it via Homebrew.
install_pgbackrest_macos() {
  local version="$1"
  local dist="$2"

  mkdir -p "$MTROOT/bin"

  # First, check if already installed on system
  if command -v pgbackrest >/dev/null 2>&1; then
    echo "pgBackRest found on system, checking version..."
    local installed_version
    installed_version=$(pgbackrest version | head -n1 | awk '{print $2}')

    if version_compare "$installed_version" "$version"; then
      echo "System pgBackRest ${installed_version} meets requirement (>= ${version}), creating symlink..."
      ln -snf "$(command -v pgbackrest)" "$MTROOT/bin/pgbackrest"
      return 0
    else
      echo "System pgBackRest ${installed_version} is older than required ${version}"
      echo "Will attempt to install/upgrade via Homebrew..."
    fi
  fi

  # Try Homebrew installation
  if command -v brew >/dev/null 2>&1; then
    echo "Installing pgBackRest ${version} via Homebrew..."

    # Try to install specific version using @version syntax
    # First check if pgbackrest is available in homebrew
    if brew install "pgbackrest@${version}" 2>/dev/null; then
      echo "Installed pgbackrest@${version}"
      ln -snf "$(brew --prefix)/bin/pgbackrest" "$MTROOT/bin/pgbackrest"
      return 0
    elif brew install pgbackrest; then
      # Fall back to latest version if specific version not available
      echo "Specific version unavailable, installed latest from Homebrew"
      ln -snf "$(brew --prefix)/bin/pgbackrest" "$MTROOT/bin/pgbackrest"
      return 0
    fi
  fi

  # If Homebrew installation failed or isn't available
  echo "ERROR: Could not install pgBackRest automatically"
  echo ""
  echo "Please install pgBackRest manually using one of these methods:"
  echo ""
  echo "Homebrew (recommended):"
  echo "  brew install pgbackrest"
  echo ""
  echo "MacPorts:"
  echo "  sudo port install pgbackrest"
  echo ""
  echo "From source:"
  echo "  # Install dependencies first:"
  echo "  brew install postgresql openssl libxml2"
  echo "  # Then download and build from: https://github.com/pgbackrest/pgbackrest/releases"
  echo ""
  exit 1
}

install_go_plugins() {
  # Reinstall protoc-gen-go and protoc-gen-go-grpc
  GOBIN=$MTROOT/bin go install google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc
}

install_go_tools() {
  # Install addlicense if not already installed
  if ! command -v addlicense >/dev/null 2>&1; then
    echo "Installing addlicense $ADDLICENSE_VERSION..."
    go install github.com/google/addlicense@"$ADDLICENSE_VERSION"
  fi
}

install_all() {
  # Create dist directory
  mkdir -p "$MTROOT/dist"

  # Install binary dependencies
  install_dep "protoc" "$PROTOC_VERSION" "$MTROOT/dist/protoc-$PROTOC_VERSION"

  # Install etcd
  install_dep "etcd" "$ETCD_VERSION" "$MTROOT/dist/etcd"

  # Install pgBackRest
  install_dep "pgbackrest" "$PGBACKREST_VERSION" "$MTROOT/dist/pgbackrest"

  # Install Go dependencies
  install_go_plugins
  install_go_tools
}

main() {
  install_all
}

main "$@"
