"use client";

import { use, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { ChevronLeft, Loader2 } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { Sparkline } from "@/components/sparkline";
import { useApi } from "@/lib/api/context";
import type { QueryStatSnapshot } from "@/lib/api/types";

const REFRESH_INTERVAL_MS = 10_000;
// Cap the response at this many fingerprints so a saturated registry can't
// ship megabytes of trend data per poll. Equivalent to the typical OLTP
// "top queries" working set; rare long-tail entries fall off naturally.
const MAX_FINGERPRINTS_PER_FETCH = 200;

// parseDurationSec converts a protojson Duration string ("0.012s", "5s", etc.)
// into seconds as a number. Returns 0 on missing/malformed input.
function parseDurationSec(s?: string): number {
  if (!s) return 0;
  const trimmed = s.endsWith("s") ? s.slice(0, -1) : s;
  const n = parseFloat(trimmed);
  return isNaN(n) ? 0 : n;
}

function formatMs(s?: string): string {
  return (parseDurationSec(s) * 1000).toFixed(2);
}

function formatRelativeTime(iso?: string): string {
  if (!iso) return "-";
  const t = new Date(iso).getTime();
  if (isNaN(t) || t === 0) return "-";
  const deltaMs = Date.now() - t;
  if (deltaMs < 0) return "just now";
  const sec = Math.round(deltaMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.round(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.round(min / 60);
  return `${hr}h ago`;
}

interface PageProps {
  params: Promise<{ cell: string; name: string }>;
}

export default function GatewayQueriesPage({ params }: PageProps) {
  const { cell, name } = use(params);
  const cellName = decodeURIComponent(cell);
  const gatewayName = decodeURIComponent(name);

  const api = useApi();
  const [queries, setQueries] = useState<QueryStatSnapshot[]>([]);
  const [tracked, setTracked] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const { snapshot } = await api.getGatewayQueries(
          {
            component: "MULTIGATEWAY",
            cell: cellName,
            name: gatewayName,
          },
          { limit: MAX_FINGERPRINTS_PER_FETCH },
        );
        if (cancelled) return;
        setQueries(snapshot.queries || []);
        setTracked(snapshot.tracked_fingerprints || 0);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load queries");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    const id = setInterval(load, REFRESH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [api, cellName, gatewayName]);

  // Derive % of runtime per row from total_duration vs the sum across all.
  const rows = useMemo(() => {
    const totalNs = queries.reduce(
      (acc, q) => acc + parseDurationSec(q.total_duration),
      0,
    );
    const filtered = filter.trim()
      ? queries.filter((q) =>
          q.normalized_sql.toLowerCase().includes(filter.toLowerCase()),
        )
      : queries;
    return filtered
      .map((q) => {
        const totalSec = parseDurationSec(q.total_duration);
        const pct = totalNs > 0 ? (totalSec / totalNs) * 100 : 0;
        return { q, totalSec, pct };
      })
      .sort((a, b) => b.pct - a.pct);
  }, [queries, filter]);

  return (
    <div className="flex flex-col gap-4 py-4 lg:py-6">
      <div className="px-4 lg:px-6 flex flex-col gap-1">
        <Link
          href="/dashboard/multigateways"
          className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
        >
          <ChevronLeft className="h-3 w-3" />
          Back to gateways
        </Link>
        <h1 className="text-2xl font-semibold tracking-tight">Per-Query Performance</h1>
        <p className="text-sm text-muted-foreground">
          <span className="font-mono">{cellName}</span> /
          <span className="font-mono"> {gatewayName}</span> · {tracked} tracked fingerprints
          · refreshes every {REFRESH_INTERVAL_MS / 1000}s
        </p>
      </div>

      <div className="px-4 lg:px-6">
        <Input
          type="text"
          placeholder="Filter queries..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="max-w-lg"
        />
      </div>

      {loading && queries.length === 0 ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          <span className="ml-2 text-muted-foreground">Loading…</span>
        </div>
      ) : error ? (
        <div className="px-4 lg:px-6">
          <p className="text-destructive">{error}</p>
        </div>
      ) : rows.length === 0 ? (
        <div className="px-4 lg:px-6 py-8">
          <p className="text-muted-foreground">
            {filter ? "No queries match the filter." : "No queries tracked yet."}
          </p>
        </div>
      ) : (
        <div className="px-4 lg:px-6">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-6">query</TableHead>
                <TableHead className="w-40">% of runtime</TableHead>
                <TableHead className="w-36 text-right">count / s</TableHead>
                <TableHead className="w-36 text-right">total time (s)</TableHead>
                <TableHead className="w-32 text-right">p50 (ms)</TableHead>
                <TableHead className="w-32 text-right">p99 (ms)</TableHead>
                <TableHead className="w-32 text-right">rows / s</TableHead>
                <TableHead className="pr-6 text-right">last seen</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map(({ q, totalSec, pct }) => (
                <TableRow key={q.fingerprint}>
                  <TableCell className="pl-6 align-top py-3">
                    <code className="text-xs whitespace-pre-wrap break-words block max-w-[640px]">
                      {q.normalized_sql || q.fingerprint}
                    </code>
                  </TableCell>
                  <TableCell className="align-top py-3">
                    <PctBar value={pct} />
                  </TableCell>
                  <TableCell className="align-top py-3 text-right">
                    <CellWithSparkline value={lastValue(q.call_rate_trends, 2)} trend={q.call_rate_trends} />
                  </TableCell>
                  <TableCell className="align-top py-3 text-right">
                    <CellWithSparkline value={totalSec.toFixed(2)} trend={q.total_time_ms_trends} />
                  </TableCell>
                  <TableCell className="align-top py-3 text-right">
                    <CellWithSparkline value={formatMs(q.p50_duration)} trend={q.p50_ms_trends} />
                  </TableCell>
                  <TableCell className="align-top py-3 text-right">
                    <CellWithSparkline value={formatMs(q.p99_duration)} trend={q.p99_ms_trends} />
                  </TableCell>
                  <TableCell className="align-top py-3 text-right">
                    <CellWithSparkline value={lastValue(q.rows_rate_trends, 2)} trend={q.rows_rate_trends} />
                  </TableCell>
                  <TableCell className="pr-6 align-top py-3 text-right text-xs text-muted-foreground">
                    {formatRelativeTime(q.last_seen)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}

// PctBar renders a horizontal gradient bar gauge for "% of runtime", with the
// numeric value above it.
function PctBar({ value }: { value: number }) {
  const clamped = Math.max(0, Math.min(100, value));
  return (
    <div className="flex flex-col gap-1">
      <span className="font-mono text-xs">{clamped.toFixed(2)}%</span>
      <div className="h-1.5 w-full rounded bg-muted">
        <div
          className="h-full rounded bg-primary"
          style={{ width: `${clamped}%` }}
        />
      </div>
    </div>
  );
}

// CellWithSparkline stacks the scalar value above a sparkline of its trend.
function CellWithSparkline({ value, trend }: { value: string; trend?: number[] }) {
  return (
    <div className="flex flex-col items-end gap-1">
      <span className="font-mono text-xs">{value}</span>
      <div className="w-24">
        <Sparkline values={trend || []} height={20} />
      </div>
    </div>
  );
}

function lastValue(arr: number[] | undefined, decimals: number): string {
  if (!arr || arr.length === 0) return "0";
  return arr[arr.length - 1].toFixed(decimals);
}
