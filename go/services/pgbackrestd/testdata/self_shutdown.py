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

"""Exercise actual pgbackrestd binary as direct PID 1: matched RPC and SIGTERM.

Usage: self_shutdown.py /absolute/path/to/linux/pgbackrestd [smoke-image]
Disposable local Docker only; no init wrapper or elevated capabilities.
The fake pgBackRest server is an owned, live synchronous process. Real 2.60
commands and sandbox enforcement are covered separately by the command suite.
"""

import json
import pathlib
import signal
import subprocess
import sys
import tempfile
import uuid

binary = pathlib.Path(sys.argv[1]).resolve()
image = sys.argv[2] if len(sys.argv) > 2 else "sidecar-linux-smoke:pgbackrest-2.60.0"


def run(args, check=True):
    """Run a bounded command and retain its combined output."""
    return subprocess.run(
        args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=check,
        timeout=45,
    )


def readiness_timeout(_signum, _frame):
    """Abort a fixture that never reports readiness."""
    raise TimeoutError("server readiness deadline exceeded")


signal.signal(signal.SIGALRM, readiness_timeout)

with tempfile.TemporaryDirectory(prefix="sidecar-self-stop-") as temp:
    root = pathlib.Path(temp)
    root.chmod(0o755)
    (root / "pgbackrest").write_text(
        '#!/bin/sh\ncase "$*" in\n'
        ' *version*) echo "pgBackRest 2.60.0";;\n'
        " *server*) echo PROCESS_READY; exec sleep 600;;\n"
        " *) exit 0;;\nesac\n"
    )
    (root / "pgbackrest").chmod(0o755)
    certs = root / "certs"
    certs.mkdir()
    for name in ["pgbackrest.crt", "pgbackrest.key", "ca.crt"]:
        (certs / name).write_text("fixture, consumed only by fake server\n")
    for mode in ["rpc", "signal"]:
        name = "sidecar-self-stop-" + uuid.uuid4().hex[:12]
        volume = name + "-state"
        attached = None
        try:
            run(["docker", "volume", "create", volume])
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
            run(
                [
                    "docker",
                    "create",
                    "--name",
                    name,
                    "--user",
                    "101:105",
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges",
                    "--tmpfs",
                    "/data:mode=1777",
                    "--tmpfs",
                    "/repo:mode=1777",
                    "--tmpfs",
                    "/tmp:mode=1777",
                    "-v",
                    volume + ":/persist",
                    "-v",
                    str(root) + ":/probes:ro",
                    "-v",
                    str(binary.parent) + ":/tests:ro",
                    "-e",
                    "PATH=/probes:/usr/bin:/bin",
                    "-e",
                    "OTEL_SDK_DISABLED=true",
                    "--entrypoint",
                    "/tests/" + binary.name,
                    image,
                    "serve",
                    "--sandbox=off",
                    "--sandbox-anchor=dir",
                    "--data-volume=/data",
                    "--pgdata=/data/pg_data",
                    "--pg-socket-dir=/data/pg_sockets",
                    "--state-dir=/persist/state",
                    "--restore-pidfile=/data/restore.pid",
                    "--wal-socket=/persist/wal.sock",
                    "--control-socket=/persist/control.sock",
                    "--location-type=filesystem",
                    "--fs-repo-path=/repo",
                    "--tls-cert-dir=/probes/certs",
                ]
            )
            attached = subprocess.Popen(
                ["docker", "start", "-a", name],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            signal.alarm(30)
            assert attached.stdout is not None
            for line in attached.stdout:
                print(line, end="", flush=True)
                if line.strip() == "PROCESS_READY":
                    break
            else:
                raise RuntimeError("server did not reach readiness")

            signal.alarm(0)

            def rpc(method, body, container=name):
                """Call this fixture instance over its private control socket."""
                return run(
                    [
                        "docker",
                        "exec",
                        container,
                        "curl",
                        "--silent",
                        "--show-error",
                        "--fail-with-body",
                        "--max-time",
                        "10",
                        "--unix-socket",
                        "/persist/control.sock",
                        "-H",
                        "Content-Type: application/json",
                        "-H",
                        "Connect-Protocol-Version: 1",
                        "-d",
                        json.dumps(body),
                        "http://unix/pgbackrestdservice.PgBackRestd/" + method,
                    ],
                    check=False,
                )

            status = rpc("Status", {})
            assert status.returncode == 0, status.stdout
            status = json.loads(status.stdout)
            assert status["instanceId"] == status["restoreIncarnation"]
            assert status["lifecycleProtocolVersion"] == 1
            for instance in ["", "stale-instance"]:
                refused = rpc("Shutdown", {"instanceId": instance})
                assert refused.returncode != 0, refused.stdout
                assert (
                    run(
                        ["docker", "inspect", "--format", "{{.State.Running}}", name]
                    ).stdout.strip()
                    == "true"
                )
            if mode == "rpc":
                accepted = rpc("Shutdown", {"instanceId": status["instanceId"]})
                assert accepted.returncode == 0, accepted.stdout
                assert json.loads(accepted.stdout)["accepted"] is True
            else:
                run(["docker", "kill", "--signal=TERM", name])
            tail, _ = attached.communicate(timeout=30)
            print(tail, end="", flush=True)
            state = json.loads(
                run(["docker", "inspect", "--format", "{{json .State}}", name]).stdout
            )
            assert (
                not state["Running"] and state["Pid"] == 0 and state["ExitCode"] == 0
            ), state
            markers = run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    "101:105",
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "-v",
                    volume + ":/persist:ro",
                    "--entrypoint",
                    "/usr/bin/find",
                    image,
                    "/persist/state",
                    "-name",
                    "worker-*.pending",
                ]
            )
            assert not markers.stdout.strip(), markers.stdout
            print(
                "PASS: direct PID-1 "
                + mode
                + " shutdown, response/exit and worker settlement",
                flush=True,
            )
        finally:
            signal.alarm(0)
            run(["docker", "rm", "-f", name], check=False)
            if attached is not None:
                attached.wait(timeout=30)
            run(["docker", "volume", "rm", volume], check=False)
