#!/usr/bin/env python3
"""Regression tests for cross-process correlation and inconclusive coverage."""
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from summarize_latency import summarize


class SummaryTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def archive(self, name, instance, events, capture_id="window", dropped=0):
        path = Path(self.directory.name) / f"{name}.zip"
        report = {
            "options": {"id": capture_id, "sample_every": 100}, "instance": instance,
            "messages_seen": 100, "messages_selected": 1, "events": events,
            "events_dropped": dropped, "unfinished_at_stop": 1,
            "propagation_dropped": 2,
        }
        manifest = {"schema_version": 1, "trace_truncated": True, "coverage_limits": ["window edges"]}
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("events.json", json.dumps(report))
            archive.writestr("manifest.json", json.dumps(manifest))
        return path

    def event(self, phase, root="a/1/1", **kwargs):
        return {"id": 1, "phase": phase, "root": root, "duration_ns": 1000000, **kwargs}

    def test_matching_uses_capture_instance_span_and_root(self):
        client = self.archive("client", "a", [self.event("rpc.operation.client")])
        server = self.archive("server", "b", [self.event("rpc.operation.server", remote_parent="a:1")])
        result = summarize([client, server])
        self.assertEqual(result["matched_rpcs"], 1)
        self.assertEqual(result["client_spans_without_server"], 0)
        self.assertEqual(result["server_spans_without_client"], 0)
        self.assertTrue(result["classification"].startswith("inconclusive"))
        self.assertEqual(result["captures"][0]["propagation_dropped"], 2)
        self.assertTrue(result["captures"][0]["trace_truncated"])
        mismatched = self.archive("mismatch", "b", [self.event("rpc.operation.server", root="a/2/1", remote_parent="a:1")])
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            summarize([client, mismatched])

    def test_missing_peers_and_separate_windows_are_not_conflated(self):
        client = self.archive("client", "a", [self.event("rpc.client")], dropped=3)
        later = self.archive("later", "a", [self.event("rpc.client")], capture_id="later")
        server = self.archive("server", "b", [self.event("rpc.server", remote_parent="a:1")], capture_id="other")
        result = summarize([client, later, server])
        self.assertEqual(result["matched_rpcs"], 0)
        self.assertEqual(result["client_spans_without_server"], 2)
        self.assertEqual(result["server_spans_without_client"], 1)
        self.assertEqual(len(result["phase_durations"]), 3)
        self.assertEqual(result["captures"][0]["dropped"], 3)
        self.assertEqual(result["captures"][0]["unfinished"], 1)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            summarize([client, client])


if __name__ == "__main__":
    unittest.main()
