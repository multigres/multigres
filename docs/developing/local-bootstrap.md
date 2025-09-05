# Local Bootstrap Guide

Guide for setting up and running a local Multigres cluster for development.

## Prerequisites

- **Go 1.25+**
- **PostgreSQL 17.6+**

### Installing PostgreSQL

**macOS:**
```bash
brew install postgresql@17
# We want to make sure postgres is not running as it will be managed by multigres
brew services stop postgresql@17 
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install postgresql-17 postgresql-client-17
```

## Quick Setup

1. **Setup environment and build:**
   ```bash
   source ./build.env
   make tools  # Installs etcd and other dependencies automatically
   make build
   ```

2. **Initialize cluster:**
   ```bash
   ./bin/multigres cluster init --config-path ./config
   ```
   This generates a `multigres.yaml` configuration file with local development settings. Inspect this configuration to check what multigres will start.

3. **Start cluster:**
   ```bash
   ./bin/multigres cluster start --config-path ./config
   ```

4. **Stop cluster:**
   ```bash
   # Normal stop (preserves data)
   ./bin/multigres cluster stop --config-path ./config
   
   # Clean stop (removes all data)
   ./bin/multigres cluster stop --clean --config-path ./config
   ```

## What Gets Started

The local cluster includes:
- **etcd** - Cluster topology backend
- **multigres** - All multigres components with a default database created.

## Directory Structure

```
./config/
├── multigres.yaml         # Configuration
├── data//                 # Data persistence for all stateful components (postgres / etcd)
├── state/                 # Service metadata state files (us)
└── logs/                  # Service logs
```

## Development Workflow

```bash
# Make changes, then rebuild and restart
make build
./bin/multigres cluster stop --config-path ./config
./bin/multigres cluster start --config-path ./config
```

## Troubleshooting

- **Port conflicts**: Check `lsof -i :2379` and kill conflicting processes
- **Service issues**: Check logs in `./config/logs/*`
- **Fresh start**: Use `./bin/multigres cluster stop --clean --config-path ./config`