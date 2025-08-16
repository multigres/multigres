#!/bin/bash
# Copyright 2025 The Multigres Authors
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

source build.env

# Multigres build tools setup script
# Installs build dependencies using install_dep pattern

# Dependency versions
PROTOC_VERSION="$PROTOC_VER"
ADDLICENSE_VERSION="$ADDLICENSE_VER"

get_platform() {
    case $(uname) in
        Linux) echo "linux";;
        Darwin) echo "osx";;
        *) echo "ERROR: unsupported platform"; exit 1;;
    esac
}

get_arch() {
    case $(uname -m) in
        x86_64) echo "x86_64";;
        aarch64) echo "aarch_64";;
        arm64) 
            case $(get_platform) in
                osx) echo "aarch_64";;
                *) echo "ERROR: unsupported architecture"; exit 1;;
            esac;;
        *) echo "ERROR: unsupported architecture"; exit 1;;
    esac
}

install_dep() {
    local name="$1"
    local version="$2"
    local dist="$3"
    
    local version_file="$dist/.installed_version"
    
    # Check if already installed with correct version
    if [[ -f "$version_file" && "$(cat "$version_file")" == "$version" ]]; then
        echo "skipping $name install. remove $dist to force re-install."
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
        *)
            echo "ERROR: unknown dependency $name"
            exit 1
            ;;
    esac
    
    # Mark as installed with this version
    echo "$version" > "$version_file"
    echo "$name installed successfully"
}

install_protoc_impl() {
    local version="$1"
    local dist="$2"
    
    local platform=$(get_platform)
    local arch=$(get_arch)
    local filename="protoc-${version}-${platform}-${arch}.zip"
    local url="https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${filename}"
    
    cd "$dist"
    
    echo "Downloading ${url}..."
    curl -L -o "${filename}" "${url}"
    
    echo "Extracting protoc..."
    unzip -q "${filename}"
    
    # protoc is now available at $dist/bin/protoc
    
    # Clean up
    rm "${filename}"
    cd - > /dev/null
}

install_go_plugins() {
    echo "Installing Go protobuf plugins..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    echo "Go protobuf plugins installed successfully"
}

install_go_tools() {
    echo "Installing Go tools..."
    go install github.com/google/addlicense@$ADDLICENSE_VERSION
    echo "Go tools installed successfully"
}

install_all() {
    echo "Setting up build tools for Multigres..."
    
    # Create dist directory
    mkdir -p "$MTROOT/dist"
    
    # Install binary dependencies
    install_dep "protoc" "$PROTOC_VERSION" "$MTROOT/dist/protoc-$PROTOC_VERSION"
    
    # Install Go dependencies
    install_go_plugins
    install_go_tools
    
    echo "Build tools setup complete!"
}

main() {
    install_all
}

main "$@"