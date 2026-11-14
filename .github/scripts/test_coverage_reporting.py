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

"""Focused GoJSON driver regression; no services or third-party Python modules.

Run with the workflow's Go toolchain:
    python3 .github/scripts/test_coverage_reporting.py

Source templates are instantiated in a temporary module outside the coverage tree.
It reproduces progress output touching testing's framing, without real test logs.
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
# Exercise the flags in the production command, not a separately maintained copy.
DRIVER = (SCRIPTS / "run-coverage-slice.sh").read_text()
FLAGS = shlex.split(DRIVER.split("if go test ", 1)[1].split("\\\n", 1)[0])


def run_fixture(*extra, fail=False):
    """Run the isolated fixture with the real coverage-driver flags."""
    with tempfile.TemporaryDirectory() as directory:
        module = Path(directory)
        for template in sorted(FIXTURE.glob("*.txt")):
            (module / template.name.removesuffix(".txt")).write_bytes(
                template.read_bytes()
            )
        profile = module / "coverage.txt"
        raw = Path(directory) / "raw.jsonl"
        command = [
            "go",
            "test",
            *FLAGS,
            "-short",
            "-count=1",
            "-cover",
            "-covermode=atomic",
            f"-coverprofile={profile}",
            *extra,
            ".",
        ]
        # Match the driver's pipefail/tee/consumer arrangement. The consumer here
        # is cat rather than tparse: this test checks JSON framing and Go status.
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
        coverage = profile.read_text()
    return result.returncode, events, coverage


def strict_results(events):
    """The pinned dorny parser's final-event invariant (no normalization)."""
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
    """Preserve structured test outcomes and coverage through the pipeline."""

    def test_framing_and_coverage(self):
        """Keep framed pass/subtest/skip events and atomic coverage."""
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

    def test_negative_control_explicit_verbose_loses_terminal(self):
        """Demonstrate the old flag combination corrupts terminal events."""
        status, events, _ = run_fixture("-v")
        self.assertEqual(status, 0)
        with self.assertRaisesRegex(ValueError, "missing elapsed"):
            strict_results(events)
        # Confirm why: testing's PASS text was swallowed by unframed progress.
        self.assertTrue(
            any(
                "progress without a newline...--- PASS: TestProgress"
                in event.get("Output", "")
                for event in events
            )
        )

    def test_failure_and_pipeline_status_are_preserved(self):
        """Keep real failures and the nonzero pipeline status visible."""
        status, events, profile = run_fixture(fail=True)
        self.assertNotEqual(status, 0)
        self.assertEqual(strict_results(events)["TestFailure"]["Action"], "fail")
        self.assertEqual(events[-1]["Action"], "fail")
        self.assertIn(
            "fixture failure must remain visible",
            "".join(event.get("Output", "") for event in events),
        )
        self.assertTrue(profile.startswith("mode: atomic\n"))

    def test_no_repair_of_truncation_or_missing_elapsed(self):
        """Reject incomplete streams instead of inferring success."""
        # Minimal redacted bad streams must still fail the strict contract.
        for events in (
            [{"Package": "fixture", "Test": "TestProgress", "Action": "run"}],
            [{"Package": "fixture", "Test": "TestProgress", "Action": "pass"}],
            [{"Package": "fixture", "Test": "TestProgress", "Action": "fail"}],
        ):
            with self.subTest(events=events), self.assertRaises(ValueError):
                strict_results(events)


if __name__ == "__main__":
    unittest.main()
