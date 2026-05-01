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
import { useApi } from "@/lib/api/context";
import type { ConsolidatorStats } from "@/lib/api/types";

const REFRESH_INTERVAL_MS = 10_000;

interface PageProps {
  params: Promise<{ cell: string; name: string }>;
}

export default function GatewayConsolidatorPage({ params }: PageProps) {
  const { cell, name } = use(params);
  const cellName = decodeURIComponent(cell);
  const gatewayName = decodeURIComponent(name);

  const api = useApi();
  const [stats, setStats] = useState<ConsolidatorStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const { stats } = await api.getGatewayConsolidator({
          component: "MULTIGATEWAY",
          cell: cellName,
          name: gatewayName,
        });
        if (cancelled) return;
        setStats(stats);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(
          err instanceof Error ? err.message : "Failed to load consolidator stats",
        );
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

  const filteredStatements = useMemo(() => {
    const all = stats?.prepared_statements || [];
    if (!filter.trim()) return all;
    const q = filter.toLowerCase();
    return all.filter(
      (s) => s.name.toLowerCase().includes(q) || s.query.toLowerCase().includes(q),
    );
  }, [stats, filter]);

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
        <h1 className="text-2xl font-semibold tracking-tight">
          Prepared Statement Consolidator
        </h1>
        <p className="text-sm text-muted-foreground">
          <span className="font-mono">{cellName}</span> /
          <span className="font-mono"> {gatewayName}</span> · refreshes every{" "}
          {REFRESH_INTERVAL_MS / 1000}s
        </p>
      </div>

      {loading && !stats ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          <span className="ml-2 text-muted-foreground">Loading…</span>
        </div>
      ) : error ? (
        <div className="px-4 lg:px-6">
          <p className="text-destructive">{error}</p>
        </div>
      ) : !stats ? null : (
        <>
          <div className="px-4 lg:px-6 grid grid-cols-1 sm:grid-cols-3 gap-3">
            <SummaryCard label="Unique statements" value={stats.unique_statements} />
            <SummaryCard label="Total references" value={stats.total_references} />
            <SummaryCard label="Connections" value={stats.connection_count} />
          </div>

          <div className="px-4 lg:px-6">
            <Input
              type="text"
              placeholder="Filter by name or query..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="max-w-lg"
            />
          </div>

          <div className="px-4 lg:px-6">
            {filteredStatements.length === 0 ? (
              <p className="text-muted-foreground py-8">
                {filter
                  ? "No statements match the filter."
                  : "No prepared statements currently held."}
              </p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="pl-6">name</TableHead>
                    <TableHead>query</TableHead>
                    <TableHead className="pr-6 text-right">refs</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredStatements.map((s) => (
                    <TableRow key={s.name}>
                      <TableCell className="pl-6 font-mono text-xs py-3 align-top">
                        {s.name}
                      </TableCell>
                      <TableCell className="py-3 align-top">
                        <code className="text-xs whitespace-pre-wrap break-words block max-w-[720px]">
                          {s.query}
                        </code>
                      </TableCell>
                      <TableCell className="pr-6 text-right font-mono text-xs py-3 align-top">
                        {s.refs}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </div>
        </>
      )}
    </div>
  );
}

function SummaryCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded border bg-card p-3">
      <p className="text-xs uppercase text-muted-foreground tracking-wide">
        {label}
      </p>
      <p className="text-2xl font-semibold font-mono">{value}</p>
    </div>
  );
}
