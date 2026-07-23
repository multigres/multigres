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
import { useApi } from "@/lib/api/context";
import { cn } from "@/lib/utils";
import type { Duration, Timestamp } from "@bufbuild/protobuf";
import type {
  PoolerStatus,
  PrimaryStatus,
  StandbyReplicationStatus,
  ID,
} from "@/lib/api/types";
import {
  PoolerType,
  PostgresStatus,
  SynchronousCommitLevel,
  SynchronousMethod,
} from "@/lib/api/types";

const REFRESH_INTERVAL_MS = 10_000;

interface PageProps {
  params: Promise<{ cell: string; name: string }>;
}

export default function PoolerReplicationPage({ params }: PageProps) {
  const { cell, name } = use(params);
  const cellName = decodeURIComponent(cell);
  const poolerName = decodeURIComponent(name);

  const api = useApi();
  const [status, setStatus] = useState<PoolerStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const resp = await api.getPoolerStatus({
          cell: cellName,
          name: poolerName,
        });
        if (cancelled) return;
        setStatus(resp.status ?? null);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(
          err instanceof Error ? err.message : "Failed to load pooler status",
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
  }, [api, cellName, poolerName]);

  return (
    <div className="flex flex-col gap-4 py-4 lg:py-6">
      <div className="px-4 lg:px-6 flex flex-col gap-1">
        <Link
          href="/dashboard/multipoolers"
          className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
        >
          <ChevronLeft className="h-3 w-3" />
          Back to poolers
        </Link>
        <h1 className="text-2xl font-semibold tracking-tight">Replication</h1>
        <p className="text-sm text-muted-foreground">
          <span className="font-mono">{cellName}</span> /
          <span className="font-mono"> {poolerName}</span> · refreshes every{" "}
          {REFRESH_INTERVAL_MS / 1000}s
        </p>
      </div>

      {loading && !status ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          <span className="ml-2 text-muted-foreground">Loading…</span>
        </div>
      ) : error ? (
        <div className="px-4 lg:px-6">
          <p className="text-destructive">{error}</p>
        </div>
      ) : !status ? null : (
        <ReplicationView status={status} />
      )}
    </div>
  );
}

function ReplicationView({ status }: { status: PoolerStatus }) {
  const role = stripPrefix(PoolerType[status.poolerType]) || "—";
  const postgresStatus = stripPrefix(PostgresStatus[status.postgresStatus]);

  return (
    <>
      <div className="px-4 lg:px-6 grid grid-cols-2 sm:grid-cols-4 gap-3">
        <SummaryCard label="Pooler type" value={role} mono />
        <SummaryCard
          label="Postgres status"
          value={postgresStatus || "—"}
          mono
        />
        <SummaryCard
          label="Postgres ready"
          value={boolText(status.postgresReady)}
          mono
        />
        <SummaryCard
          label="WAL position"
          value={status.walPosition || "—"}
          mono
        />
      </div>

      {status.primaryStatus ? (
        <PrimarySection primary={status.primaryStatus} />
      ) : null}

      {status.replicationStatus ? (
        <ReplicaSection replication={status.replicationStatus} />
      ) : null}

      {!status.primaryStatus && !status.replicationStatus ? (
        <div className="px-4 lg:px-6">
          <p className="text-muted-foreground">
            No replication status available (PostgreSQL may not be connected).
          </p>
        </div>
      ) : null}
    </>
  );
}

function PrimarySection({ primary }: { primary: PrimaryStatus }) {
  const sync = primary.syncReplicationConfig;
  const syncCommit = sync
    ? stripPrefix(
        SynchronousCommitLevel[sync.synchronousCommit],
        "SYNCHRONOUS_COMMIT_",
      )
    : "";
  const syncMethod = sync
    ? stripPrefix(SynchronousMethod[sync.synchronousMethod])
    : "";
  const followerSet = useMemo(() => {
    const s = new Set<string>();
    for (const f of primary.connectedFollowers ?? []) {
      s.add(idKey(f));
    }
    return s;
  }, [primary.connectedFollowers]);

  const standbyRows = useMemo(() => {
    const ids = sync?.standbyIds ?? [];
    const names = sync?.standbyApplicationNames ?? [];
    return ids.map((id, i) => ({
      id,
      applicationName: names[i],
      connected: followerSet.has(idKey(id)),
    }));
  }, [sync, followerSet]);

  return (
    <>
      <SectionHeading>Primary</SectionHeading>

      <div className="px-4 lg:px-6 grid grid-cols-2 sm:grid-cols-4 gap-3">
        <SummaryCard
          label="synchronous_commit"
          value={syncCommit || "—"}
          mono
        />
        <SummaryCard label="Sync method" value={syncMethod || "—"} mono />
        <SummaryCard label="num_sync" value={sync?.numSync ?? "—"} mono />
        <SummaryCard
          label="WAL senders"
          value={
            primary.maxWalSenders != null
              ? `${primary.connectedFollowers?.length ?? 0} connected / ${primary.maxWalSenders} max`
              : `${primary.connectedFollowers?.length ?? 0} connected`
          }
          mono
        />
      </div>

      <div className="px-4 lg:px-6">
        <h3 className="text-sm font-medium mb-2">Standbys</h3>
        {standbyRows.length === 0 ? (
          <p className="text-muted-foreground text-sm">
            No standbys configured in synchronous_standby_names.
          </p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-6">Cell</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>application_name</TableHead>
                <TableHead className="pr-6 text-right">Streaming</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {standbyRows.map((row) => (
                <TableRow key={idKey(row.id)}>
                  <TableCell className="pl-6 font-mono text-xs py-3">
                    {row.id.cell}
                  </TableCell>
                  <TableCell className="font-mono text-xs py-3">
                    {row.id.name}
                  </TableCell>
                  <TableCell className="font-mono text-xs py-3">
                    {row.applicationName || "—"}
                  </TableCell>
                  <TableCell className="pr-6 text-right py-3">
                    <ConnectedBadge connected={row.connected} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>
    </>
  );
}

function ReplicaSection({
  replication,
}: {
  replication: StandbyReplicationStatus;
}) {
  const conn = replication.primaryConnInfo;
  const waiting = replication.walReceiverStatus || "—";
  const lastMsgAgo = formatAgo(replication.lastMsgReceiveTime);

  return (
    <>
      <SectionHeading>Replica</SectionHeading>

      <div className="px-4 lg:px-6 grid grid-cols-2 sm:grid-cols-4 gap-3">
        <SummaryCard
          label="WAL receiver"
          value={waiting}
          mono
          tone={walReceiverTone(replication.walReceiverStatus)}
        />
        <SummaryCard label="Last msg from primary" value={lastMsgAgo} mono />
        <SummaryCard
          label="status_interval"
          value={formatDuration(replication.walReceiverStatusInterval)}
          mono
        />
        <SummaryCard
          label="receiver_timeout"
          value={formatDuration(replication.walReceiverTimeout)}
          mono
        />
      </div>

      <SectionHeading subtle>Replay state</SectionHeading>
      <div className="px-4 lg:px-6 grid grid-cols-2 sm:grid-cols-4 gap-3">
        <SummaryCard
          label="last_receive_lsn"
          value={replication.lastReceiveLsn || "—"}
          mono
        />
        <SummaryCard
          label="last_replay_lsn"
          value={replication.lastReplayLsn || "—"}
          mono
        />
        <SummaryCard
          label="Replay paused"
          value={
            replication.isWalReplayPaused
              ? replication.walReplayPauseState || "paused"
              : "not paused"
          }
          mono
        />
        <SummaryCard label="Lag" value={formatDuration(replication.lag)} mono />
      </div>

      <SectionHeading subtle>primary_conninfo</SectionHeading>
      {conn ? (
        <div className="px-4 lg:px-6 flex flex-col gap-3">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <SummaryCard label="host" value={conn.host || "—"} mono />
            <SummaryCard label="port" value={conn.port ?? "—"} mono />
            <SummaryCard label="user" value={conn.user || "—"} mono />
            <SummaryCard
              label="application_name"
              value={conn.applicationName || "—"}
              mono
            />
          </div>
          {conn.raw ? (
            <div className="rounded border bg-card p-3">
              <p className="text-xs uppercase text-muted-foreground tracking-wide mb-1">
                raw (password redacted)
              </p>
              <code className="text-xs whitespace-pre-wrap break-words block font-mono">
                {conn.raw}
              </code>
            </div>
          ) : null}
        </div>
      ) : (
        <div className="px-4 lg:px-6">
          <p className="text-muted-foreground text-sm">
            primary_conninfo is empty (no streaming source configured).
          </p>
        </div>
      )}

      {replication.lastXactReplayTimestamp ? (
        <div className="px-4 lg:px-6">
          <p className="text-xs text-muted-foreground">
            Last replayed commit at{" "}
            <span className="font-mono">
              {replication.lastXactReplayTimestamp}
            </span>
          </p>
        </div>
      ) : null}
    </>
  );
}

function SectionHeading({
  children,
  subtle,
}: {
  children: React.ReactNode;
  subtle?: boolean;
}) {
  return (
    <div className="px-4 lg:px-6 mt-2">
      <h2
        className={cn(
          subtle ? "text-sm font-medium" : "text-lg font-semibold",
          "tracking-tight",
        )}
      >
        {children}
      </h2>
    </div>
  );
}

function SummaryCard({
  label,
  value,
  mono,
  tone,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
  tone?: "success" | "warn" | "neutral";
}) {
  return (
    <div className="rounded border bg-card p-3">
      <p className="text-xs uppercase text-muted-foreground tracking-wide">
        {label}
      </p>
      <p
        className={cn(
          "text-xl font-semibold",
          mono && "font-mono",
          tone === "success" && "text-emerald-400",
          tone === "warn" && "text-amber-400",
        )}
      >
        {value}
      </p>
    </div>
  );
}

function ConnectedBadge({ connected }: { connected: boolean }) {
  return (
    <span
      className={cn(
        "font-mono text-xs px-1.5 py-0.5 rounded",
        connected
          ? "bg-emerald-500/20 text-emerald-400"
          : "bg-muted text-muted-foreground",
      )}
    >
      {connected ? "yes" : "no"}
    </span>
  );
}

function idKey(id: ID): string {
  return `${id.cell}/${id.name}`;
}

function stripPrefix(name: string | undefined, prefix = ""): string {
  if (!name) return "";
  return name.startsWith(prefix) ? name.slice(prefix.length) : name;
}

function boolText(v: boolean | undefined): string {
  if (v === undefined) return "—";
  return v ? "yes" : "no";
}

function formatDuration(d: Duration | undefined): string {
  if (!d) return "—";
  const seconds = Number(d.seconds) + d.nanos / 1e9;
  return `${seconds}s`;
}

function formatAgo(ts: Timestamp | undefined): string {
  if (!ts) return "—";
  const then = ts.toDate().getTime();
  const seconds = Math.max(0, Math.round((Date.now() - then) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

function walReceiverTone(
  status: string | undefined,
): "success" | "warn" | "neutral" | undefined {
  if (!status) return "warn";
  if (status === "streaming") return "success";
  if (status === "stopping" || status === "waiting") return "warn";
  return undefined;
}
