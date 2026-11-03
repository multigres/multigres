#!/usr/bin/env python3
"""Summarize bounded latency archives; never infer a causal inside/outside verdict."""

import argparse
import collections
import json
import statistics
import zipfile


def read_archive(path):
    with zipfile.ZipFile(path) as archive:
        results = []
        for name in ("events.json", "manifest.json"):
            if archive.getinfo(name).file_size > 128 * 1024 * 1024:
                raise ValueError(f"{path}: oversized {name}")
            results.append(json.loads(archive.read(name)))
        return results


def percentile(values, percent):
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return (ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)) / 1e6


def summarize(paths):
    phases = collections.defaultdict(list)
    clients = {}
    servers = []
    captures = []
    keys = set()
    for path in paths:
        report, manifest = read_archive(path)
        if manifest.get("schema_version") != 1:
            raise ValueError("unsupported capture schema version")
        key = (report["options"]["id"], report["instance"])
        if key in keys:
            raise ValueError(f"duplicate process/capture: {key}")
        keys.add(key)
        captures.append({
            "path": str(path), "id": key[0], "instance": key[1],
            "sample_every": report["options"]["sample_every"],
            "messages_seen": report["messages_seen"],
            "messages_selected": report["messages_selected"],
            "events": len(report["events"]), "dropped": report["events_dropped"],
            "unfinished": report["unfinished_at_stop"],
            "propagation_dropped": report.get("propagation_dropped", 0),
            "trace_truncated": manifest["trace_truncated"],
            "coverage_limits": manifest["coverage_limits"],
        })
        for event in report["events"]:
            phases[(key[0], report["instance"], event["phase"])].append(event["duration_ns"])
            if event["phase"].startswith("rpc.client") or event["phase"] == "rpc.operation.client":
                clients[(key[0], f'{key[1]}:{event["id"]}')] = event
            if event.get("remote_parent"):
                servers.append((key[0], event))
    matched = []
    matched_clients = set()
    missing_clients = 0
    for capture_id, server in servers:
        key = (capture_id, server["remote_parent"])
        client = clients.get(key)
        if client is None:
            missing_clients += 1
            continue
        if client["root"] != server["root"]:
            raise ValueError("RPC identity mismatch")
        matched_clients.add(key)
        matched.append({
            "root": server["root"], "client_span": key[1],
            "client_ms": client["duration_ns"] / 1e6,
            "server_ms": server["duration_ns"] / 1e6,
            # This includes Multigres transport/scheduling work too. It is NOT
            # evidence that the difference occurred outside Multigres.
            "client_minus_server_ms": (client["duration_ns"] - server["duration_ns"]) / 1e6,
        })
    return {
        "classification": "inconclusive_without_matched_client_and_transport_evidence",
        "captures": captures,
        "phase_durations": [
            {"capture_id": capture_id, "instance": instance, "phase": phase, "count": len(values),
             "mean_ms": statistics.mean(values) / 1e6,
             **{f"p{p}_ms": percentile(values, p) for p in (50, 90, 99)}}
            for (capture_id, instance, phase), values in sorted(phases.items())
        ],
        "matched_rpcs": len(matched),
        "client_spans_without_server": len(clients) - len(matched_clients),
        "server_spans_without_client": missing_clients,
        "rpc_pairs": matched,
        "interpretation": "Intervals overlap; do not add phase totals or percentiles. Missing peers/window edges/sampling prevent complete accounting. RPC duration differences include transport and scheduling within Multigres as well as network time.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", nargs="+")
    args = parser.parse_args()
    try:
        result = summarize(args.archives)
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
