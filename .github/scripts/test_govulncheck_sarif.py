#!/usr/bin/env python3
# Copyright 2026 Supabase, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline regression tests: python3 .github/scripts/test_govulncheck_sarif.py.

Requires bash and jq. Execute the actual workflow's literal shell block, not a
copy of its filter. No scanner, network access, or SARIF upload is performed.
"""

import copy
import json
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows/govulncheck.yaml"


def deduplication_script():
    # Deliberately require the existing literal-block shape rather than silently
    # testing a different step if the workflow is restructured.
    matches = re.findall(
        r"^      - name: Deduplicate SARIF[^\n]*\n"
        r"(?:        #[^\n]*\n)*        run: \|\n"
        r"((?:          [^\n]*\n)+)",
        WORKFLOW.read_text(),
        re.MULTILINE,
    )
    if len(matches) != 1:
        raise AssertionError("expected exactly one literal SARIF deduplication step")
    return textwrap.dedent(matches[0])


class SarifDeduplicationTest(unittest.TestCase):
    def transform(self, raw, succeeds=True):
        with tempfile.TemporaryDirectory(prefix="sarif-test-") as directory:
            report = Path(directory) / "govulncheck.sarif"
            report.write_bytes(raw)
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c",
                 deduplication_script()],
                cwd=directory, capture_output=True, text=True, check=False,
            )
            if succeeds:
                self.assertEqual(result.returncode, 0, result.stderr)
            else:
                self.assertNotEqual(result.returncode, 0)
            return report.read_bytes()

    def transform_json(self, value):
        return json.loads(self.transform(json.dumps(value).encode()))

    def test_exact_duplicates_only_and_idempotence(self):
        stack = {"message": {"text": "call path"}, "frames": [
            {"location": {"physicalLocation": {"artifactLocation": {"uri": "a.go"},
                                                   "region": {"startLine": 12}}}},
            {"location": {"message": {"text": "caller"}}},
        ]}
        different_line = copy.deepcopy(stack)
        different_line["frames"][0]["location"]["physicalLocation"]["region"]["startLine"] = 13
        different_order = copy.deepcopy(stack)
        different_order["frames"].reverse()
        different_message = copy.deepcopy(stack)
        different_message["message"]["text"] = "another path"
        distinct = [stack, different_line, different_order, different_message]
        finding = {"ruleId": "GO-TEST", "message": {"text": "vulnerable"},
                   "stacks": distinct + [copy.deepcopy(stack)]}
        # Equal objects with different key insertion order are also duplicates.
        finding["stacks"].append(dict(reversed(list(stack.items()))))
        value = {"version": "2.1.0", "runs": [{
            "tool": {"driver": {"name": "govulncheck", "rules": [{
                "id": "GO-TEST", "properties": {"tags": ["CVE-B", "CVE-A", "CVE-B"],
                                                       "other": "preserved"}}]}},
            "results": [finding, copy.deepcopy(finding),
                        {"ruleId": "GO-OTHER", "message": {"text": "distinct finding"}}],
            "invocations": [{"executionSuccessful": False}],
        }]}
        expected = copy.deepcopy(value)
        expected["runs"][0]["tool"]["driver"]["rules"][0]["properties"]["tags"] = ["CVE-A", "CVE-B"]
        actual = self.transform_json(value)
        for i in (0, 1):
            self.assertCountEqual(actual["runs"][0]["results"][i]["stacks"], distinct)
            # Compare the entire remainder, including duplicate result retention.
            actual["runs"][0]["results"][i]["stacks"] = distinct
            expected["runs"][0]["results"][i]["stacks"] = distinct
        self.assertEqual(actual, expected)
        once = self.transform(json.dumps(value).encode())
        self.assertEqual(self.transform(once), once)

    def test_optional_metadata_and_results(self):
        rules = [{"id": "missing"}, {"id": "null-properties", "properties": None},
                 {"id": "empty-properties", "properties": {}},
                 {"id": "null-tags", "properties": {"tags": None}},
                 {"id": "empty-tags", "properties": {"tags": []}}]
        runs = []
        for driver in ({"name": "scanner"}, {"name": "scanner", "rules": None},
                       {"name": "scanner", "rules": []}, {"name": "scanner", "rules": rules}):
            for results in ({}, {"results": None}, {"results": []},
                            {"results": [{"message": {"text": "no stacks"}},
                                         {"stacks": None}, {"stacks": []}]}):
                runs.append({"tool": {"driver": driver}, **results})
        value = {"runs": runs}
        self.assertEqual(self.transform_json(value), value)

    def test_non_json_scanner_failures_untouched(self):
        for raw in (b"", b"govulncheck: package loading failed\n", b'{"runs": ['):
            with self.subTest(raw=raw):
                self.assertEqual(self.transform(raw), raw)

    def test_invalid_array_types_fail_without_replacing_report(self):
        for value in (
            {"runs": [{"tool": {"driver": {"rules": 42}}}]},
            {"runs": [{"results": [{"stacks": "not-an-array"}]}]},
        ):
            raw = json.dumps(value).encode()
            with self.subTest(value=value):
                self.assertEqual(self.transform(raw, succeeds=False), raw)


if __name__ == "__main__":
    unittest.main()
