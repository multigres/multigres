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

## Documentation

The website and documentation is located in the `/site` folder.

### Install dependencies

```
pnpm i
```

### Local Development

```
pnpm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```
pnpm build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

The site is automatically deployed when a Pull Request is merged into the `main` branch. 