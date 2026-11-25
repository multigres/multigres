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

"""Checks that run-coverage-slice.sh's `go test -json` stream stays reportable.

Runs a fixture Go package with the exact `go test` arguments the coverage
driver uses and checks that every test's final JSON event is a terminal event
carrying Elapsed, which the golang-json test reporter requires. The fixture
prints progress without a trailing newline, which breaks that guarantee if the
driver passes -v alongside -json. Needs only Go and the Python standard library:
    python3 .github/scripts/test_coverage_reporting.py

The fixture sources are .txt templates copied into a temporary module, so they
stay out of this repository's package discovery and coverage totals.
"""

import json
import os
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
FIXTURE = SCRIPTS / "testdata" / "coverage-reporting"
DRIVER = SCRIPTS / "run-coverage-slice.sh"


def driver_go_test_args():
    """Return every literal argument of the driver's `go test` command.

    The whole backslash-continued command up to the first pipe is parsed, so a
    flag added on any of its lines reaches the fixture run. Shell expansions
    (the -run slice and the package list) are dropped: the fixture supplies its
    own package.
    """
    text = DRIVER.read_text()
    marker = "if go test "
    if marker not in text:
        raise RuntimeError(f"{DRIVER} no longer contains {marker!r}")
    command = text.split(marker, 1)[1].split("|", 1)[0]
    args = shlex.split(command.replace("\\\n", " "))
    return [arg for arg in args if "$" not in arg]


ARGS = driver_go_test_args()
PROFILE = next(
    arg.removeprefix("-coverprofile=")
    for arg in ARGS
    if arg.startswith("-coverprofile=")
)


def run_fixture(*extra, fail=False):
    """Run the fixture package through the driver's arguments and pipeline."""
    with tempfile.TemporaryDirectory() as directory:
        module = Path(directory)
        for template in sorted(FIXTURE.glob("*.txt")):
            (module / template.name.removesuffix(".txt")).write_bytes(
                template.read_bytes()
            )
        raw = module / "raw.jsonl"
        command = ["go", "test", *ARGS, "-short", "-count=1", *extra, "."]
        # The driver's pipefail/tee arrangement, with cat in place of tparse:
        # this checks the JSON stream and the exit status, not tparse's output.
        result = subprocess.run(
            [
                "bash",
                "-c",
                'set -o pipefail; "$@" | tee "$RAW" | cat',
                "coverage-fixture",
                *command,
            ],
            cwd=module,
            env={
                **os.environ,
                "RAW": str(raw),
                "REPORTING_FIXTURE_FAIL": "1" if fail else "0",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        events = [json.loads(line) for line in raw.read_text().splitlines()]
        coverage = (module / PROFILE).read_text()
    return result.returncode, events, coverage


def strict_results(events):
    """Return each test's final event, rejecting streams the reporter rejects.

    The golang-json reporter fails when a test's final event lacks Elapsed. This
    also requires that event to be pass, skip or fail, so a stream cut off
    mid-test is rejected rather than read as an outcome.
    """
    groups = {}
    for event in events:
        if event.get("Test"):
            groups.setdefault((event["Package"], event["Test"]), []).append(event)
    for group in groups.values():
        last = group[-1]
        if "Elapsed" not in last:
            raise ValueError("missing elapsed on final test event")
        if last["Action"] not in ("pass", "skip", "fail"):
            raise ValueError("missing terminal event")
    return {test: group[-1] for (_, test), group in groups.items()}


class CoverageReportingTest(unittest.TestCase):
    """Test outcomes and coverage survive the coverage driver's pipeline."""

    def test_framing_and_coverage(self):
        """Pass, subtest and skip events end framed, and coverage is written."""
        status, events, profile = run_fixture()
        self.assertEqual(status, 0)
        results = strict_results(events)
        for name in (
            "TestProgress",
            "TestSubtests",
            "TestSubtests/first",
            "TestSubtests/second",
        ):
            self.assertEqual(results[name]["Action"], "pass")
        self.assertEqual(results["TestSkip"]["Action"], "skip")
        self.assertEqual(events[-1]["Action"], "pass")
        self.assertIn("progress.go:", profile)
        self.assertTrue(profile.startswith("mode: atomic\n"))
        self.assertTrue(
            any(int(line.split()[-1]) > 0 for line in profile.splitlines()[1:])
        )

    def test_explicit_verbose_loses_terminal_event(self):
        """Adding -v to the driver's arguments breaks the reporter's input."""
        status, events, _ = run_fixture("-v")
        self.assertEqual(status, 0)
        with self.assertRaisesRegex(ValueError, "missing elapsed"):
            strict_results(events)
        # The cause: unframed progress output swallowed testing's PASS line.
        self.assertTrue(
            any(
                "progress without a newline...--- PASS: TestProgress"
                in event.get("Output", "")
                for event in events
            )
        )

    def test_failure_and_pipeline_status_are_preserved(self):
        """A failing test is reported as failed and fails the pipeline."""
        status, events, profile = run_fixture(fail=True)
        self.assertNotEqual(status, 0)
        self.assertEqual(strict_results(events)["TestFailure"]["Action"], "fail")
        self.assertEqual(events[-1]["Action"], "fail")
        self.assertIn(
            "fixture failure must remain visible",
            "".join(event.get("Output", "") for event in events),
        )
        self.assertTrue(profile.startswith("mode: atomic\n"))

    def test_strict_results_rejects_incomplete_streams(self):
        """The checker rejects a truncated stream or a final event without Elapsed."""
        for events in (
            [{"Package": "fixture", "Test": "TestProgress", "Action": "run"}],
            [{"Package": "fixture", "Test": "TestProgress", "Action": "pass"}],
            [{"Package": "fixture", "Test": "TestProgress", "Action": "fail"}],
        ):
            with self.subTest(events=events), self.assertRaises(ValueError):
                strict_results(events)


if __name__ == "__main__":
    unittest.main()
