#!/usr/bin/env python3
# Copyright 2026 Supabase, Inc.
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

"""Exercise PID-1 restore crash/restart with retained private state.

Usage: python3 namespace_restart.py /absolute/path/to/linux/pgbackrestd.test [image]
Creates only UUID-named disposable local Docker containers/volume. No privileged
mode, capabilities, writable cgroups, host PID namespace or seccomp override.
The Go test's explicit readiness event, attach completion/runtime inspect and its second-start
assertions are the barriers; this driver does not sleep or guess exit timing.
"""

import json
import os
import pathlib
import subprocess
import sys
import uuid

binary = pathlib.Path(sys.argv[1]).resolve()
image = sys.argv[2] if len(sys.argv) > 2 else "sidecar-linux-smoke:pgbackrest-2.60.0"
name = "sidecar-ownership-" + uuid.uuid4().hex[:12]
volume = name + "-state"
entry = "/tests/" + binary.name


def run(args, check=True):
    """Run a bounded command and retain its combined output."""
    result = subprocess.run(
        args,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
    )
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args}")
    return result


created_volume = False
created_container = False
attached = None
try:
    run(
        [
            "docker",
            "volume",
            "create",
            "--label",
            "purpose=sidecar-ownership-test",
            volume,
        ]
    )
    created_volume = True
    # Prepare only this fresh volume. chmod of a directory owned by UID 0 needs
    # no capabilities; the workload and all worker tests run as UID 101.
    run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0",
            "--cap-drop",
            "ALL",
            "--read-only",
            "--network",
            "none",
            "-v",
            volume + ":/persist",
            "--entrypoint",
            "/bin/chmod",
            image,
            "1777",
            "/persist",
        ]
    )
    common = [
        "--user",
        "101:105",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--hostname",
        name,
        "--tmpfs",
        "/tmp:rw,exec,nosuid,nodev,size=256m,mode=1777",
        "-v",
        str(binary.parent) + ":/tests:ro",
        "-v",
        volume + ":/persist",
        "-e",
        "PGBACKRESTD_NAMESPACE_RESTART_ROOT=/persist",
        "--entrypoint",
        entry,
    ]
    test_args = [
        "-test.run=^TestContainerNamespaceRestartRecovery$",
        "-test.v",
        "-test.timeout=120s",
    ]
    run(["docker", "create", "--name", name, *common, image, *test_args])
    created_container = True
    attached = subprocess.Popen(
        ["docker", "start", "-a", name],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert attached.stdout is not None
    for line in attached.stdout:
        print(line, end="", flush=True)
        if line.strip() == "CONTAINER_RESTORE_READY":
            break
    else:
        raise RuntimeError("restore never reached the explicit readiness barrier")
    # Even a fresh PID-1 sibling with matching hostname cannot retire a live
    # owner's records: the original instance flock must still reject it.
    if os.environ.get("PGBACKRESTD_RESTART_SKIP_PROBE") != "1":
        run(
            [
                "docker",
                "run",
                "--rm",
                *common,
                "-e",
                "PGBACKRESTD_NAMESPACE_RESTART_PROBE=1",
                image,
                *test_args,
            ]
        )
    run(["docker", "kill", "--signal=KILL", name])
    tail, _ = attached.communicate(timeout=30)
    if tail:
        print(tail, end="", flush=True)
    state = run(["docker", "inspect", "--format", "{{json .State}}", name])
    state = json.loads(state.stdout)
    assert (
        not state["Running"] and state["Pid"] == 0 and state["ExitCode"] == 137
    ), state
    print(
        "Runtime confirmed old PID namespace stopped; restarting SAME container and volume",
        flush=True,
    )
    # No marker, journal, sentinel or PGDATA reset between these starts.
    run(["docker", "start", "-a", name])
    print("PASS: retained-state PID-1 restart and fenced restore retry", flush=True)
    if os.environ.get("PGBACKRESTD_RESTART_SKIP_PROBE") != "1":
        print("PASS: competing live-owner namespace refused", flush=True)
finally:
    if created_container:
        run(["docker", "rm", "-f", name], check=False)
    if attached is not None and attached.poll() is None:
        attached.wait(timeout=30)
    if created_volume:
        run(["docker", "volume", "rm", volume], check=False)
