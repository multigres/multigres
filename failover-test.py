#!/usr/bin/env python3

import os
import sys
import time
import subprocess
import requests
import yaml
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple
from dataclasses import dataclass

# Configuration
MULTIADMIN_URL = os.getenv("MULTIADMIN_URL", "http://localhost:15000")
CONFIG_FILE = "./multigres_local/multigres.yaml"
PGCTLD_BIN = "./bin/pgctld"
MULTIGRES_BIN = "./bin/multigres"
CHECK_INTERVAL = 1  # seconds

# Colors
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color

@dataclass
class PoolerInfo:
    cell: str
    service_id: str
    pooler_dir: str
    pg_port: int

def log_info(msg: str):
    print(f"{Colors.BLUE}[INFO]{Colors.NC} {msg}", file=sys.stderr)

def log_success(msg: str):
    print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {msg}", file=sys.stderr)

def log_warn(msg: str):
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}", file=sys.stderr)

def log_error(msg: str):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}", file=sys.stderr)

def load_config() -> Dict:
    """Load and parse the YAML config file."""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

def get_pooler_config(service_id: str) -> Optional[PoolerInfo]:
    """Get pooler configuration from YAML config file."""
    config = load_config()
    cells = config.get('provisioner-config', {}).get('cells', {})

    for cell_name, cell_config in cells.items():
        multipooler = cell_config.get('multipooler', {})
        if multipooler.get('service-id') == service_id:
            return PoolerInfo(
                cell=cell_name,
                service_id=service_id,
                pooler_dir=multipooler.get('pooler-dir'),
                pg_port=multipooler.get('pg-port')
            )

    return None

def get_poolers() -> Dict:
    """Get all poolers from the API."""
    response = requests.get(f"{MULTIADMIN_URL}/api/v1/poolers")
    response.raise_for_status()
    return response.json()

def disable_postgres_monitoring_on_all_poolers():
    """Disable postgres monitoring on all poolers to prevent automatic restarts."""
    log_info("Disabling PostgreSQL monitoring on all poolers...")

    poolers_data = get_poolers()
    poolers = poolers_data.get('poolers', [])

    if not poolers:
        log_warn("No poolers found")
        return

    for pooler in poolers:
        cell = pooler['id']['cell']
        name = pooler['id']['name']
        pooler_name = f"multipooler-{cell}-{name}"

        log_info(f"  Disabling monitoring on: {pooler_name}")
        try:
            result = subprocess.run(
                [MULTIGRES_BIN, "disablepostgresmonitor", "--pooler", pooler_name],
                capture_output=True,
                text=True,
                check=True
            )
            log_success(f"    ✓ Disabled monitoring on {pooler_name}")
        except subprocess.CalledProcessError as e:
            log_error(f"    ✗ Failed to disable monitoring on {pooler_name}: {e.stderr}")
            raise

    log_success(f"Disabled monitoring on {len(poolers)} pooler(s)")
    print()

def get_pooler_status(cell: str, service_id: str) -> Dict:
    """Get status for a specific pooler."""
    response = requests.get(f"{MULTIADMIN_URL}/api/v1/poolers/{cell}/{service_id}/status")
    response.raise_for_status()
    return response.json()

def find_primary() -> Optional[PoolerInfo]:
    """Find the current healthy primary pooler."""
    log_info("Searching for current primary...")

    poolers_data = get_poolers()
    primaries = [p for p in poolers_data.get('poolers', []) if p.get('type') == 'PRIMARY']

    if not primaries:
        log_error("No primary found!")
        return None

    if len(primaries) > 1:
        log_warn("Multiple primaries found! This indicates a split-brain situation.")

    # Check each primary to find a healthy one
    for primary in primaries:
        cell = primary['id']['cell']
        service_id = primary['id']['name']

        # Get status to verify it's healthy
        status_data = get_pooler_status(cell, service_id)
        status = status_data.get('status', {})
        postgres_running = status.get('postgres_running', False)
        is_ready = status.get('primary_status', {}).get('ready', False)

        if postgres_running and is_ready:
            # Get the pooler config from our local config
            config = get_pooler_config(service_id)
            if not config:
                log_error(f"Could not find config for service_id: {service_id}")
                continue

            log_success(f"Found primary: {cell}/{service_id} (port: {config.pg_port})")
            log_info(f"  - PostgreSQL running: {postgres_running}")
            log_info(f"  - Ready: {is_ready}")

            return config

    log_error("No healthy primary found!")
    return None

def wait_for_new_primary(old_service_id: str, max_attempts: int = 60) -> bool:
    """Wait for a new primary to be elected."""
    log_info("Waiting for new primary to be elected...")

    for attempt in range(max_attempts):
        try:
            poolers_data = get_poolers()
            primaries = [p for p in poolers_data.get('poolers', []) if p.get('type') == 'PRIMARY']

            # Check each primary to find a healthy one that's not the old one
            for primary in primaries:
                cell = primary['id']['cell']
                service_id = primary['id']['name']

                # Skip the old primary
                if service_id == old_service_id:
                    continue

                # Check if this primary is healthy
                status_data = get_pooler_status(cell, service_id)
                status = status_data.get('status', {})
                postgres_running = status.get('postgres_running', False)
                is_ready = status.get('primary_status', {}).get('ready', False)

                if postgres_running and is_ready:
                    print()  # New line after dots
                    log_success(f"New primary elected: {cell}/{service_id}")
                    return True

        except Exception as e:
            pass  # Ignore transient errors during failover

        print(".", end="", flush=True, file=sys.stderr)
        time.sleep(CHECK_INTERVAL)

    print()  # New line after dots
    log_error("Timeout waiting for new primary")
    return False

def wait_for_replica_health(cell: str, service_id: str, max_attempts: int = 60) -> bool:
    """Wait for a pooler to become a healthy replica connected to the new primary."""
    log_info(f"Waiting for {cell}/{service_id} to become a healthy replica...")

    last_lsn = None

    for attempt in range(max_attempts):
        try:
            # Get the replica's status
            status_data = get_pooler_status(cell, service_id)
            status = status_data.get('status', {})

            pooler_type = status.get('pooler_type')
            postgres_running = status.get('postgres_running', False)
            repl_status = status.get('replication_status')

            if (pooler_type == 'REPLICA' and
                postgres_running and
                repl_status is not None):

                last_receive_lsn = repl_status.get('last_receive_lsn')
                last_replay_lsn = repl_status.get('last_replay_lsn')
                is_paused = repl_status.get('is_wal_replay_paused', False)

                if last_receive_lsn and last_replay_lsn and not is_paused:
                    # Find the current primary and verify this replica is connected
                    poolers_data = get_poolers()
                    primaries = [p for p in poolers_data.get('poolers', [])
                                if p.get('type') == 'PRIMARY']

                    # Check each primary to find a healthy one
                    for primary in primaries:
                        primary_cell = primary['id']['cell']
                        primary_service_id = primary['id']['name']

                        try:
                            primary_status_data = get_pooler_status(primary_cell, primary_service_id)
                            primary_status = primary_status_data.get('status', {})

                            if not primary_status.get('postgres_running', False):
                                continue

                            primary_ready = primary_status.get('primary_status', {}).get('ready', False)

                            if not primary_ready:
                                continue

                            # Check if this replica is in the primary's connected followers
                            connected_followers = primary_status.get('primary_status', {}).get('connected_followers', [])

                            is_connected = any(
                                follower.get('cell') == cell and follower.get('name') == service_id
                                for follower in connected_followers
                            )

                            if is_connected:
                                # Verify LSN is advancing (not stuck)
                                if last_lsn is not None and last_replay_lsn == last_lsn:
                                    # LSN hasn't advanced, keep waiting
                                    pass
                                else:
                                    print()  # New line after dots
                                    log_success(f"Replica {cell}/{service_id} is healthy and replicating")
                                    log_info(f"  - Connected to primary: {primary_cell}/{primary_service_id}")
                                    log_info(f"  - Last receive LSN: {last_receive_lsn}")
                                    log_info(f"  - Last replay LSN: {last_replay_lsn}")
                                    return True

                                # Track LSN for next iteration
                                last_lsn = last_replay_lsn

                        except Exception:
                            continue  # Try next primary

        except Exception as e:
            pass  # Ignore transient errors

        print(".", end="", flush=True, file=sys.stderr)
        time.sleep(CHECK_INTERVAL)

    print()  # New line after dots
    log_error("Timeout waiting for replica to become healthy")
    return False

def run_sql_query(socket_dir: str, pg_port: int, query: str) -> Optional[str]:
    """Execute a SQL query on PostgreSQL via Unix socket and return the result."""
    try:
        result = subprocess.run(
            ["psql", "-h", socket_dir, "-p", str(pg_port), "-U", "postgres", "-d", "postgres",
             "-t", "-A", "-c", query],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        return None

def print_timeline_info(pooler_info: PoolerInfo, label: str = ""):
    """Print timeline information for a pooler."""
    socket_dir = f"{pooler_info.pooler_dir}/pg_sockets"

    if label:
        log_info(f"{label}")

    # Wait a moment for postgres to be queryable
    for attempt in range(10):
        checkpoint_result = run_sql_query(socket_dir, pooler_info.pg_port,
                                         "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();")
        if checkpoint_result:
            parts = checkpoint_result.split('|')
            if len(parts) == 2:
                print(f"  {Colors.YELLOW}Timeline ID: {parts[0]}{Colors.NC}")
                print(f"  Checkpoint Redo LSN: {parts[1]}")

                # Also check if in recovery and WAL receiver status
                recovery_result = run_sql_query(socket_dir, pooler_info.pg_port,
                                               "SELECT pg_is_in_recovery();")
                if recovery_result:
                    in_recovery = recovery_result == 't'
                    print(f"  In Recovery: {in_recovery}")

                    if in_recovery:
                        receiver_result = run_sql_query(socket_dir, pooler_info.pg_port,
                                                       "SELECT status, received_tli FROM pg_stat_wal_receiver;")
                        if receiver_result:
                            recv_parts = receiver_result.split('|')
                            if len(recv_parts) == 2:
                                print(f"  WAL Receiver Status: {recv_parts[0]}")
                                print(f"  Received Timeline: {recv_parts[1]}")
                            else:
                                print(f"  WAL Receiver: {Colors.YELLOW}no active receiver{Colors.NC}")
                return
            break
        time.sleep(0.5)

    log_warn("Could not query timeline information")

def print_replication_status():
    """Print detailed replication status for all poolers."""
    print()
    print("=" * 60)
    log_info("Replication Status")
    print("=" * 60)

    try:
        poolers_data = get_poolers()
        poolers = poolers_data.get('poolers', [])

        # Find the primary
        primary = None
        primary_service_id = None
        all_poolers = []

        # First pass: collect all poolers and find the healthy primary
        for pooler in poolers:
            cell = pooler['id']['cell']
            service_id = pooler['id']['name']
            pooler_type = pooler.get('type')
            pg_port = pooler.get('port_map', {}).get('postgres')
            pooler_dir = pooler.get('pooler_dir')

            if not pooler_dir:
                continue

            socket_dir = f"{pooler_dir}/pg_sockets"
            all_poolers.append((cell, service_id, pg_port, socket_dir, pooler_type))

            if pooler_type == 'PRIMARY':
                # Verify it's actually healthy
                try:
                    status_data = get_pooler_status(cell, service_id)
                    status = status_data.get('status', {})
                    if status.get('postgres_running') and status.get('primary_status', {}).get('ready'):
                        primary = (cell, service_id, pg_port, socket_dir)
                        primary_service_id = service_id
                except Exception:
                    pass

        # Second pass: all non-primary poolers are treated as replicas
        replicas = [(cell, sid, port, sock) for cell, sid, port, sock, ptype in all_poolers
                    if sid != primary_service_id]

        # Get primary timeline info
        if primary:
            cell, service_id, pg_port, socket_dir = primary
            print()
            print(f"{Colors.GREEN}PRIMARY: {cell}/{service_id} (port: {pg_port}){Colors.NC}")

            # Get checkpoint info from pg_control_checkpoint (this is the canonical timeline)
            checkpoint_result = run_sql_query(socket_dir, pg_port, "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();")
            if checkpoint_result:
                parts = checkpoint_result.split('|')
                if len(parts) == 2:
                    print(f"  Timeline ID: {parts[0]}")
                    print(f"  Checkpoint Redo LSN: {parts[1]}")
                else:
                    print(f"  Checkpoint: {Colors.YELLOW}[unexpected format]{Colors.NC}")
            else:
                print(f"  Checkpoint: {Colors.RED}[query failed]{Colors.NC}")
        else:
            print()
            print(f"{Colors.RED}No healthy primary found!{Colors.NC}")

        # Get replica info
        for cell, service_id, pg_port, socket_dir in replicas:
            print()
            print(f"{Colors.BLUE}REPLICA: {cell}/{service_id} (port: {pg_port}){Colors.NC}")

            # Check if PostgreSQL is running
            try:
                status_data = get_pooler_status(cell, service_id)
                status = status_data.get('status', {})
                if not status.get('postgres_running'):
                    print(f"  {Colors.RED}PostgreSQL not running{Colors.NC}")
                    continue
            except Exception:
                print(f"  {Colors.RED}Cannot get status{Colors.NC}")
                continue

            # Get replication receiver status
            receiver_result = run_sql_query(socket_dir, pg_port, "SELECT status, received_tli FROM pg_stat_wal_receiver;")
            if receiver_result:
                parts = receiver_result.split('|')
                if len(parts) == 2:
                    print(f"  Receiver Status: {parts[0]}")
                    print(f"  Received Timeline: {parts[1]}")
                else:
                    print(f"  Receiver: {Colors.YELLOW}[no active receiver]{Colors.NC}")
            else:
                print(f"  Receiver: {Colors.RED}[query failed]{Colors.NC}")

            # Get checkpoint info
            checkpoint_result = run_sql_query(socket_dir, pg_port, "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();")
            if checkpoint_result:
                parts = checkpoint_result.split('|')
                if len(parts) == 2:
                    print(f"  Checkpoint Timeline: {parts[0]}")
                    print(f"  Checkpoint Redo LSN: {parts[1]}")
                else:
                    print(f"  Checkpoint: {Colors.YELLOW}[unexpected format]{Colors.NC}")
            else:
                print(f"  Checkpoint: {Colors.RED}[query failed]{Colors.NC}")

        print()
        print("=" * 60)

    except Exception as e:
        log_error(f"Failed to get replication status: {e}")

def stop_pooler(pooler_dir: str, pg_port: int):
    """Stop a pooler."""
    log_info(f"Stopping pooler: {pooler_dir} (port: {pg_port})")
    subprocess.run(
        [PGCTLD_BIN, "stop", "--pooler-dir", pooler_dir, "--pg-port", str(pg_port)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    log_success(f"Pooler stopped at {timestamp}")

def start_pooler(pooler_dir: str, pg_port: int):
    """Start a pooler."""
    log_info(f"Starting pooler: {pooler_dir} (port: {pg_port})")
    subprocess.run(
        [PGCTLD_BIN, "start", "--pooler-dir", pooler_dir, "--pg-port", str(pg_port)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    log_success(f"Pooler started at {timestamp}")

def failover_loop():
    """Main failover loop."""
    iteration = 1

    while True:
        print()
        print("=" * 38)
        log_info(f"Failover Test - Iteration {iteration}")
        print("=" * 38)
        print()

        # Find current primary
        primary_info = find_primary()
        if not primary_info:
            log_error("Could not find primary. Exiting.")
            return 1

        print()
        log_warn(f"About to kill primary: {primary_info.cell}/{primary_info.service_id}")
        log_info(f"  Pooler dir: {primary_info.pooler_dir}")
        log_info(f"  Port: {primary_info.pg_port}")
        print()

        # Ask user for confirmation
        try:
            response = input("Kill this primary? (y/n): ").strip().lower()
            if response != 'y':
                log_info("Skipping this iteration")
                continue
        except (KeyboardInterrupt, EOFError):
            print()
            log_info("Exiting...")
            return 0

        # Stop the primary
        try:
            stop_pooler(primary_info.pooler_dir, primary_info.pg_port)
        except subprocess.CalledProcessError as e:
            log_error(f"Failed to stop pooler: {e}")
            return 1

        # Wait for new primary
        if not wait_for_new_primary(primary_info.service_id):
            log_error("Failed to detect new primary. Manual intervention required.")
            return 1

        # Let the system restart postgres organically (through multiorch recovery)
        log_info("Waiting for system to restart postgres organically...")
        print()

        # Wait for the old primary to be restarted by the system and become a healthy replica
        # Give it up to 60 seconds for the system to detect, restart, and replicate
        if not wait_for_replica_health(primary_info.cell, primary_info.service_id, max_attempts=60):
            log_error("Replica did not become healthy within 60 seconds!")
            log_error("Printing final replication status for diagnostics...")
            print_replication_status()
            return 1

        log_success(f"Failover iteration {iteration} complete!")

        # Print detailed replication status
        print_replication_status()

        iteration += 1
        time.sleep(2)

def main():
    log_info("Multigres Failover Test Script")
    log_info(f"MultiAdmin API: {MULTIADMIN_URL}")
    log_info(f"Config file: {CONFIG_FILE}")
    print()

    # Verify prerequisites
    if not os.path.exists(CONFIG_FILE):
        log_error(f"Config file not found: {CONFIG_FILE}")
        return 1

    if not os.path.exists(PGCTLD_BIN):
        log_error(f"pgctld binary not found: {PGCTLD_BIN}")
        return 1

    if not os.path.exists(MULTIGRES_BIN):
        log_error(f"multigres binary not found: {MULTIGRES_BIN}")
        return 1

    # Test API connectivity
    try:
        requests.get(f"{MULTIADMIN_URL}/api/v1/poolers", timeout=5)
    except requests.RequestException as e:
        log_error(f"Cannot connect to MultiAdmin API at {MULTIADMIN_URL}")
        log_error("Make sure the multigres cluster is running.")
        return 1

    log_success("All prerequisites satisfied")
    print()

    # Disable PostgreSQL monitoring on all poolers to prevent automatic restarts
    try:
        disable_postgres_monitoring_on_all_poolers()
    except Exception as e:
        log_error(f"Failed to disable postgres monitoring: {e}")
        return 1

    # Start the failover loop
    return failover_loop()

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        log_info("Interrupted by user")
        sys.exit(0)
