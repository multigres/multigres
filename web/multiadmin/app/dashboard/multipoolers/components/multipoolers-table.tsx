"use client";

import { useEffect, useState, useMemo } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Loader2, ExternalLink } from "lucide-react";
import { useApi } from "@/lib/api/context";
import type { MultiPooler } from "@/lib/api/types";
import { cn } from "@/lib/utils";

function PoolerTypeBadge({ type }: { type?: string }) {
  if (!type) return <>-</>;

  const isPrimary = type === "PRIMARY";
  return (
    <span
      className={cn(
        "font-mono text-xs px-1.5 py-0.5 rounded",
        isPrimary
          ? "bg-blue-500/20 text-blue-400"
          : "bg-purple-500/20 text-purple-400",
      )}
    >
      {type}
    </span>
  );
}

function ServingStatusBadge({ status }: { status?: string }) {
  if (!status) return <>-</>;

  const isServing = status.toUpperCase() === "SERVING";
  return (
    <Badge
      variant={isServing ? "default" : "destructive"}
      className="font-mono text-xs"
    >
      {status}
    </Badge>
  );
}

export function MultiPoolersTable() {
  const api = useApi();
  const [poolers, setPoolers] = useState<MultiPooler[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchPoolers() {
      try {
        setLoading(true);
        setError(null);

        const { poolers } = await api.getPoolers();
        setPoolers(poolers || []);
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to fetch multipoolers",
        );
      } finally {
        setLoading(false);
      }
    }

    fetchPoolers();
  }, [api]);

  const filteredPoolers = useMemo(() => {
    if (!searchQuery.trim()) {
      return poolers;
    }

    const query = searchQuery.toLowerCase();
    return poolers.filter((pooler) => {
      const searchableText = [
        pooler.id?.cell || "",
        pooler.id?.name || "",
        pooler.database || "",
        pooler.table_group || "",
        pooler.shard || "",
        pooler.type || "",
        pooler.serving_status || "",
        pooler.hostname || "",
        pooler.port_map?.postgres?.toString() || "",
        pooler.port_map?.grpc?.toString() || "",
        pooler.port_map?.http?.toString() || "",
      ]
        .join(" ")
        .toLowerCase();

      return searchableText.includes(query);
    });
  }, [searchQuery, poolers]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">
          Loading multipoolers...
        </span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-destructive">{error}</p>
      </div>
    );
  }

  return (
    <>
      <div className="px-4 lg:px-6 py-4">
        <Input
          type="text"
          placeholder="Search by cell, name, database, table group, shard, type, status, or hostname..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="max-w-lg"
        />
      </div>

      {filteredPoolers.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12">
          {searchQuery ? (
            <>
              <p className="text-muted-foreground">
                No poolers match &quot;{searchQuery}&quot;
              </p>
              <button
                onClick={() => setSearchQuery("")}
                className="mt-2 text-sm text-primary hover:underline"
              >
                Clear search
              </button>
            </>
          ) : (
            <p className="text-muted-foreground">No multipoolers found</p>
          )}
        </div>
      ) : (
        <div className="px-4 lg:px-6">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-6">Cell</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Database</TableHead>
                <TableHead>Table Group</TableHead>
                <TableHead>Shard</TableHead>
                <TableHead className="text-center">Type</TableHead>
                <TableHead className="text-center">Status</TableHead>
                <TableHead>Hostname</TableHead>
                <TableHead className="text-right">gRPC Port</TableHead>
                <TableHead className="pr-6">Dashboard</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredPoolers.map((pooler, idx) => {
                const dashboardUrl = `/proxy/pool/${pooler.id?.cell}/${pooler.id?.name}`;

                return (
                  <TableRow key={pooler.id?.name || idx}>
                    <TableCell className="pl-6 font-mono text-xs py-3">
                      {pooler.id?.cell || "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs py-3">
                      {pooler.id?.name || "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs py-3">
                      {pooler.database || "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs py-3">
                      {pooler.table_group || "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs py-3">
                      {pooler.shard || "-"}
                    </TableCell>
                    <TableCell className="text-center py-3">
                      <PoolerTypeBadge type={pooler.type} />
                    </TableCell>
                    <TableCell className="text-center py-3">
                      <ServingStatusBadge status={pooler.serving_status} />
                    </TableCell>
                    <TableCell className="font-mono text-xs py-3">
                      {pooler.hostname || "-"}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs py-3">
                      {pooler.port_map?.grpc || "-"}
                    </TableCell>
                    <TableCell className="pr-6 py-3">
                      <a
                        href={dashboardUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-1 text-primary hover:underline text-xs"
                      >
                        <ExternalLink className="h-3 w-3" />
                        View
                      </a>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </>
  );
}
