#!/usr/bin/env python3

import os
import sys
import time
import subprocess
import requests
import argparse
from datetime import datetime, timezone
from typing import Optional, Dict
from dataclasses import dataclass

# Configuration
MULTIADMIN_URL = os.getenv("MULTIADMIN_URL", "http://localhost:18000")
MULTIADMIN_GRPC = os.getenv("MULTIADMIN_GRPC", "localhost:18070")
KUBECTL_CONTEXT = os.getenv("KUBECTL_CONTEXT", "kind-multidemo")
KUBERNETES_NAMESPACE = os.getenv("KUBERNETES_NAMESPACE", "default")
CHECK_INTERVAL = 1  # seconds

# Kubernetes configuration
PGCTLD_BIN = "/usr/local/bin/pgctld"
POOLER_DIR = "/data"

# Local multigres CLI (relative to demo/k8s directory)
MULTIGRES_BIN = os.path.join(os.path.dirname(__file__), "..", "..", "bin", "multigres")

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
    pod_name: str

def log_info(msg: str):
    print(f"{Colors.BLUE}[INFO]{Colors.NC} {msg}", file=sys.stderr)

def log_success(msg: str):
    print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {msg}", file=sys.stderr)

def log_warn(msg: str):
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}", file=sys.stderr)

def log_error(msg: str):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}", file=sys.stderr)

def get_poolers() -> Dict:
    """Get all poolers from the API."""
    response = requests.get(f"{MULTIADMIN_URL}/api/v1/poolers")
    response.raise_for_status()
    return response.json()

def get_pooler_status(cell: str, service_id: str) -> Dict:
    """Get status for a specific pooler."""
    response = requests.get(f"{MULTIADMIN_URL}/api/v1/poolers/{cell}/{service_id}/status")
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
        service_id = pooler['id']['name']
        # In k8s, the service_id from the API is already the full identifier (e.g., 'multipooler-zone1-0')
        # The pooler name for the CLI is: multipooler-{cell}-{service_id}
        pooler_name = f"multipooler-{cell}-{service_id}"

        log_info(f"  Disabling monitoring on: {pooler_name} (cell={cell}, service_id={service_id})")
        try:
            result = subprocess.run(
                [MULTIGRES_BIN, "disablepostgresmonitor",
                 "--pooler", pooler_name,
                 "--admin-server", MULTIADMIN_GRPC],
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

def get_pooler_info(cell: str, service_id: str) -> PoolerInfo:
    """Create PoolerInfo from cell and service_id.

    In k8s, the service_id already contains the full identifier (e.g., 'multipooler-zone1-0'),
    and the pod name matches this exactly.
    """
    # Pod name is just the service_id since it already includes everything
    pod_name = service_id
    return PoolerInfo(cell=cell, service_id=service_id, pod_name=pod_name)

def run_sql_query_in_pod(pod_name: str, query: str) -> Optional[str]:
    """Execute a SQL query in a Kubernetes pod via kubectl exec."""
    try:
        # Run psql inside the pgctld container using unix socket
        cmd = [
            "kubectl", "--context", KUBECTL_CONTEXT,
            "exec", "-n", KUBERNETES_NAMESPACE,
            f"pod/{pod_name}",
            "-c", "pgctld", "--",
            "psql", "-h", f"{POOLER_DIR}/pg_sockets", "-p", "5432",
            "-U", "postgres", "-d", "postgres",
            "-t", "-A", "-c", query
        ]

        result = subprocess.run(
            cmd,
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
    if label:
        log_info(f"{label}")

    # Wait a moment for postgres to be queryable
    for attempt in range(10):
        checkpoint_result = run_sql_query_in_pod(
            pooler_info.pod_name,
            "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"
        )
        if checkpoint_result:
            parts = checkpoint_result.split('|')
            if len(parts) == 2:
                print(f"  {Colors.YELLOW}Timeline ID: {parts[0]}{Colors.NC}")
                print(f"  Checkpoint Redo LSN: {parts[1]}")

                # Also check if in recovery and WAL receiver status
                recovery_result = run_sql_query_in_pod(
                    pooler_info.pod_name,
                    "SELECT pg_is_in_recovery();"
                )
                if recovery_result:
                    in_recovery = recovery_result == 't'
                    print(f"  In Recovery: {in_recovery}")

                    if in_recovery:
                        receiver_result = run_sql_query_in_pod(
                            pooler_info.pod_name,
                            "SELECT status, received_tli FROM pg_stat_wal_receiver;"
                        )
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
            # Pod name is just the service_id (same as get_pooler_info())
            pod_name = service_id

            all_poolers.append((cell, service_id, pod_name, pooler_type))

            if pooler_type == 'PRIMARY':
                # Verify it's actually healthy
                try:
                    status_data = get_pooler_status(cell, service_id)
                    status = status_data.get('status', {})
                    if status.get('postgres_running') and status.get('primary_status', {}).get('ready'):
                        primary = (cell, service_id, pod_name)
                        primary_service_id = service_id
                except Exception:
                    pass

        # Second pass: all non-primary poolers are treated as replicas
        replicas = [(cell, sid, pod) for cell, sid, pod, ptype in all_poolers
                    if sid != primary_service_id]

        # Get primary timeline info
        if primary:
            cell, service_id, pod_name = primary
            print()
            print(f"{Colors.GREEN}PRIMARY: {cell}/{service_id} (pod: {pod_name}){Colors.NC}")

            # Get checkpoint info from pg_control_checkpoint (this is the canonical timeline)
            checkpoint_result = run_sql_query_in_pod(
                pod_name,
                "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"
            )
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
        for cell, service_id, pod_name in replicas:
            print()
            print(f"{Colors.BLUE}REPLICA: {cell}/{service_id} (pod: {pod_name}){Colors.NC}")

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
            receiver_result = run_sql_query_in_pod(
                pod_name,
                "SELECT status, received_tli FROM pg_stat_wal_receiver;"
            )
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
            checkpoint_result = run_sql_query_in_pod(
                pod_name,
                "SELECT timeline_id, redo_lsn FROM pg_control_checkpoint();"
            )
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
            pooler_info = get_pooler_info(cell, service_id)
            log_success(f"Found primary: {cell}/{service_id} (pod: {pooler_info.pod_name})")
            log_info(f"  - PostgreSQL running: {postgres_running}")
            log_info(f"  - Ready: {is_ready}")
            return pooler_info

    log_error("No healthy primary found!")
    return None

def wait_for_new_primary(old_service_id: str, max_attempts: int = 60, debug: bool = False) -> bool:
    """Wait for a new primary to be elected."""
    log_info("Waiting for new primary to be elected...")

    for attempt in range(max_attempts):
        try:
            poolers_data = get_poolers()
            primaries = [p for p in poolers_data.get('poolers', []) if p.get('type') == 'PRIMARY']

            if debug and attempt % 10 == 0:  # Log every 10 attempts
                print(f"\n  [DEBUG] Found {len(primaries)} PRIMARY pooler(s)", file=sys.stderr)

            # Check each primary to find a healthy one that's not the old one
            for primary in primaries:
                cell = primary['id']['cell']
                service_id = primary['id']['name']

                # Skip the old primary
                if service_id == old_service_id:
                    if debug and attempt % 10 == 0:
                        print(f"  [DEBUG] Skipping old primary: {service_id}", file=sys.stderr)
                    continue

                if debug and attempt % 10 == 0:
                    print(f"  [DEBUG] Checking candidate: {service_id}", file=sys.stderr)

                # Check if this primary is healthy
                status_data = get_pooler_status(cell, service_id)
                status = status_data.get('status', {})
                postgres_running = status.get('postgres_running', False)
                is_ready = status.get('primary_status', {}).get('ready', False)

                if debug and attempt % 10 == 0:
                    print(f"  [DEBUG]   postgres_running={postgres_running}, ready={is_ready}", file=sys.stderr)

                if postgres_running and is_ready:
                    print()  # New line after dots
                    log_success(f"New primary elected: {cell}/{service_id}")
                    return True

        except Exception as e:
            if debug and attempt % 10 == 0:
                print(f"\n  [DEBUG] Error: {e}", file=sys.stderr)
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

        except Exception:
            pass  # Ignore transient errors

        print(".", end="", flush=True, file=sys.stderr)
        time.sleep(CHECK_INTERVAL)

    print()  # New line after dots
    log_error("Timeout waiting for replica to become healthy")
    return False

def stop_pooler(pooler_info: PoolerInfo):
    """Stop a pooler using kubectl exec."""
    log_info(f"Stopping pooler: {pooler_info.pod_name}")

    cmd = [
        "kubectl", "--context", KUBECTL_CONTEXT,
        "exec", "-n", KUBERNETES_NAMESPACE,
        f"pod/{pooler_info.pod_name}",
        "-c", "pgctld", "--",
        PGCTLD_BIN, "stop",
        "--pooler-dir", POOLER_DIR
    ]

    subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    log_success(f"Pooler stopped at {timestamp}")

def start_pooler(pooler_info: PoolerInfo):
    """Start a pooler using kubectl exec."""
    log_info(f"Starting pooler: {pooler_info.pod_name}")

    cmd = [
        "kubectl", "--context", KUBECTL_CONTEXT,
        "exec", "-n", KUBERNETES_NAMESPACE,
        f"pod/{pooler_info.pod_name}",
        "-c", "pgctld", "--",
        PGCTLD_BIN, "start",
        "--pooler-dir", POOLER_DIR
    ]

    subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    log_success(f"Pooler started at {timestamp}")

def failover_loop(auto_yes: bool = False, debug: bool = False):
    """Main failover loop.

    Args:
        auto_yes: If True, automatically proceed without asking for confirmation
        debug: If True, enable debug logging
    """
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
        log_info(f"  Pod: {primary_info.pod_name}")
        print()

        # Ask user for confirmation (unless --yes flag is used)
        if not auto_yes:
            try:
                response = input("Kill this primary? (y/n): ").strip().lower()
                if response != 'y':
                    log_info("Skipping this iteration")
                    continue
            except (KeyboardInterrupt, EOFError):
                print()
                log_info("Exiting...")
                return 0
        else:
            log_info("Auto-yes enabled, proceeding automatically...")

        # Stop the primary
        try:
            stop_pooler(primary_info)
        except subprocess.CalledProcessError as e:
            log_error(f"Failed to stop pooler: {e}")
            return 1

        # Wait for new primary
        if not wait_for_new_primary(primary_info.service_id, debug=debug):
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

        # Re-disable monitoring for the next iteration (it may have been re-enabled)
        try:
            disable_postgres_monitoring_on_all_poolers()
        except Exception as e:
            log_error(f"Failed to re-disable postgres monitoring: {e}")
            return 1

        iteration += 1
        time.sleep(2)

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Multigres Failover Test Script for Kubernetes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run interactive mode (ask before each failover)
  ./failover-test.py

  # Run automatic mode (continuous failover without prompts)
  ./failover-test.py --yes

  # Use custom admin URL
  MULTIADMIN_URL=http://localhost:8000 ./failover-test.py --yes
        """
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Automatically proceed with failovers without asking for confirmation'
    )
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Enable debug logging to diagnose issues'
    )
    args = parser.parse_args()

    log_info("Multigres Failover Test Script (Kubernetes)")
    log_info(f"MultiAdmin API: {MULTIADMIN_URL}")
    log_info(f"Kubectl context: {KUBECTL_CONTEXT}")
    log_info(f"Namespace: {KUBERNETES_NAMESPACE}")
    if args.yes:
        log_warn("Auto-yes mode enabled - will continuously kill primaries without confirmation")
    if args.debug:
        log_info("Debug mode enabled - will show detailed diagnostic information")
    print()

    # Verify prerequisites
    if not os.path.exists(MULTIGRES_BIN):
        log_error(f"multigres binary not found: {MULTIGRES_BIN}")
        log_error("Please run 'make build' first")
        return 1

    # Verify kubectl is available and can connect to cluster
    try:
        result = subprocess.run(
            ["kubectl", "--context", KUBECTL_CONTEXT, "cluster-info"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True
        )
    except FileNotFoundError:
        log_error("kubectl not found in PATH")
        log_error("Please ensure kubectl is installed")
        return 1
    except subprocess.CalledProcessError as e:
        log_error(f"kubectl cannot connect to cluster: {e.stderr}")
        log_error(f"Please verify the cluster context '{KUBECTL_CONTEXT}' exists")
        return 1

    # Test API connectivity
    try:
        requests.get(f"{MULTIADMIN_URL}/api/v1/poolers", timeout=5)
    except requests.RequestException as e:
        log_error(f"Cannot connect to MultiAdmin API at {MULTIADMIN_URL}")
        log_error("Make sure port-forwarding is set up:")
        log_error(f"  kubectl --context {KUBECTL_CONTEXT} port-forward service/multiadmin 18000:18000")
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
    return failover_loop(auto_yes=args.yes, debug=args.debug)

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        log_info("Interrupted by user")
        sys.exit(0)
