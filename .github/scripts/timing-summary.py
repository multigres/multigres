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
#
# Reads integration-test-results.jsonl (go test -json output), extracts
# timing rows emitted by shardsetup.TimingCollector, and writes a
# mean±99.9%CI / p50/p95/p99 markdown table to $GITHUB_STEP_SUMMARY.
#
# Usage: python3 .github/scripts/timing-summary.py
"""Summarise timing stats from integration test output into a markdown table."""

# pylint: disable=invalid-name  # script filename uses hyphens (conventional for CLI scripts)

import json
import math
import os
import re
import sys
from collections import defaultdict
from typing import IO

# t-distribution critical values at the 99.95th percentile (for a 99.9% two-sided
# CI on the mean), keyed by degrees of freedom (n-1).  Values are taken from
# standard t-tables.  For df > 120 the normal approximation 3.291 is used.
_T_CRIT_99_95: dict[int, float] = {
    1: 636.619,
    2: 31.598,
    3: 12.924,
    4: 8.610,
    5: 6.869,
    6: 5.959,
    7: 5.408,
    8: 5.041,
    9: 4.781,
    10: 4.587,
    12: 4.318,
    15: 4.073,
    20: 3.850,
    25: 3.725,
    30: 3.646,
    40: 3.551,
    60: 3.460,
    120: 3.373,
}


def t_crit_999(df: int) -> float:
    """Return the 99.95th-percentile t critical value for the given degrees of freedom.

    Uses the nearest entry with df' ≤ df (conservative — slightly widens the
    interval).  Falls back to the normal-distribution value 3.291 for df > 120.
    """
    if df >= 120:
        return 3.291
    for k in sorted(_T_CRIT_99_95, reverse=True):
        if df >= k:
            return _T_CRIT_99_95[k]
    return _T_CRIT_99_95[1]


def mean_ci_999(values: list[float]) -> tuple[float, float]:
    """Return (mean, half_width) for a 99.9% CI on the mean, assuming normality.

    For n=1 the half-width is 0.0 (a CI cannot be computed from a single sample).
    """
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    margin = t_crit_999(n - 1) * math.sqrt(variance) / math.sqrt(n)
    return mean, margin


def clean_label(label: str) -> str:
    """Normalise labels that vary by pooler instance.

    "manager ready: pooler-N"          → "manager ready"
    "failover: pooler-N → new primary" → "failover"
    """
    label = re.sub(r": pooler-\d+$", "", label)
    label = re.sub(r": pooler-\d+ →.*$", "", label)
    return label.strip()


def percentile(values: list[float], p: float) -> float:
    """Return the p-th percentile of values using linear interpolation."""
    s = sorted(values)
    idx = (len(s) - 1) * p / 100
    lo = int(idx)
    if lo + 1 >= len(s):
        return s[lo]
    return s[lo] + (s[lo + 1] - s[lo]) * (idx - lo)


def parse_duration_seconds(d: str) -> float:
    """Parse a Go time.Duration string (e.g. '30s', '1m0s') into seconds."""
    total = 0.0
    for value, unit in re.findall(r"([\d.]+)([a-zµ]+)", d):
        v = float(value)
        if unit == "h":
            total += v * 3600
        elif unit == "m":
            total += v * 60
        elif unit == "s":
            total += v
        elif unit == "ms":
            total += v / 1000
        elif unit in ("us", "µs"):
            total += v / 1_000_000
    return total


def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as a compact duration string."""
    if seconds < 1.0:
        return f"{seconds * 1000:.0f}ms"
    return f"{seconds:.1f}s"


def format_cell(elapsed_seconds: float, limit_seconds: float) -> str:
    """Format a percentile cell as 'Xs (P%)' showing elapsed time and percent of timeout."""
    pct = elapsed_seconds / limit_seconds * 100.0
    return f"{format_elapsed(elapsed_seconds)} ({pct:.0f}%)"


def format_ci_cell(mean_s: float, margin_s: float, limit_s: float, n: int) -> str:
    """Format a mean±CI cell as 'Xs ±Ys (P%, n=N)'.

    Shows the mean elapsed time, the 99.9% CI half-width, the mean as a
    percentage of the timeout, and the sample size so the reliability of the
    interval is immediately visible.
    """
    pct = mean_s / limit_s * 100.0
    return (
        f"{format_elapsed(mean_s)} ±{format_elapsed(margin_s)}" f" ({pct:.0f}%, n={n})"
    )


def status_circle(
    p95: float, p99: float, warn: float = 70.0, crit: float = 90.0
) -> str:
    """Return a colored circle emoji reflecting the worst percentile bucket.

    🔴  p99 >= crit  (default 90 % of timeout)
    🟡  p95 >= warn  (default 70 % of timeout)
    🟢  otherwise
    """
    if p99 >= crit:
        return "🔴"
    if p95 >= warn:
        return "🟡"
    return "🟢"


def main() -> None:
    """Parse timing rows from the JSONL test output and write a summary table."""
    jsonl_path = "integration-test-results.jsonl"
    data: dict[str, list[float]] = defaultdict(list)  # label → [elapsed_seconds, ...]
    limits: dict[str, str] = {}  # label → limit string (e.g. "30s")

    try:
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("Action") != "output":
                    continue

                # Strip the "    file.go:42: " prefix added by t.Log/t.Logf.
                # Use a single trailing space (not \s*) to preserve the two
                # leading spaces that are part of the timing row format.
                stripped = re.sub(r"^\s+\S+:\d+: ", "", obj.get("Output", "")).rstrip(
                    "\n"
                )

                # Match timing rows emitted by TimingCollector.Report:
                #   "  <label>   <elapsed> / <limit>   <pct>%"
                m = re.match(
                    r"^  (.+?)\s{2,}(\S+)\s*/\s*(\S+)\s+([\d.]+)%\s*$", stripped
                )
                if not m:
                    continue

                label = clean_label(m.group(1))
                data[label].append(parse_duration_seconds(m.group(2)))
                limits[label] = m.group(3)

    except FileNotFoundError:
        print(f"No {jsonl_path} found — skipping timing summary", file=sys.stderr)
        return

    if not data:
        print("No timing rows found — skipping timing summary", file=sys.stderr)
        return

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    out: IO[str]
    if not summary_path:
        print("GITHUB_STEP_SUMMARY not set — printing to stdout instead")
        out = sys.stdout
    else:
        out = open(
            summary_path, "a", encoding="utf-8"
        )  # pylint: disable=consider-using-with

    try:
        out.write("## Timing Summary\n\n")
        out.write(
            "| Operation | Timeout | mean ±99.9%CI | min | p50 | p95 | p99 | max |\n"
        )
        out.write("|---|---|---|---|---|---|---|---|\n")
        for label in sorted(data):
            vals = data[label]
            limit_s = parse_duration_seconds(limits[label])
            mean_s, margin_s = mean_ci_999(vals)
            p50v = percentile(vals, 50)
            p95v = percentile(vals, 95)
            p99v = percentile(vals, 99)
            circle = status_circle(p95v / limit_s * 100.0, p99v / limit_s * 100.0)
            out.write(
                f"| {circle} {label} | {limits[label]}"
                f" | {format_ci_cell(mean_s, margin_s, limit_s, len(vals))}"
                f" | {format_cell(min(vals), limit_s)}"
                f" | {format_cell(p50v, limit_s)}"
                f" | {format_cell(p95v, limit_s)}"
                f" | {format_cell(p99v, limit_s)}"
                f" | {format_cell(max(vals), limit_s)} |\n"
            )
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()
