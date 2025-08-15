# Developing Multigres

This document provides instructions for building and testing the Multigres project in a local setup.

## Prerequisites

You need to install:
- Go (version 1.25 or later)

All other build dependencies (like protoc) are automatically installed by the build system.

## Setup

To install build tools and dependencies:

```bash
make tools
```

## Building

```bash
make build
```

This builds the Go binaries and places them in the `bin/` directory.

## Protocol Buffers

To generate protobuf files:

```bash
make proto
```

Generated `.pb.go` files are placed in the `pb/` directory.

## Testing

To run all tests:

```bash
make test
```

## Cleaning

```bash
make clean
```

This removes the `bin/` directory but preserves generated protobuf files and build tools.