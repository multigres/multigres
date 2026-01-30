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
import { Loader2 } from "lucide-react";
import { useApi } from "@/lib/api/context";
import type { MultiOrch } from "@/lib/api/types";

export function MultiOrchTable() {
  const api = useApi();
  const [orchs, setOrchs] = useState<MultiOrch[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchOrchs() {
      try {
        setLoading(true);
        setError(null);

        const { orchs } = await api.getOrchs();
        setOrchs(orchs || []);
      } catch (err) {
        setError(
          err instanceof Error
            ? err.message
            : "Failed to fetch multiorchestrators",
        );
      } finally {
        setLoading(false);
      }
    }

    fetchOrchs();
  }, [api]);

  const filteredOrchs = useMemo(() => {
    if (!searchQuery.trim()) {
      return orchs;
    }

    const query = searchQuery.toLowerCase();
    return orchs.filter((orch) => {
      const searchableText = [
        orch.id?.cell || "",
        orch.id?.name || "",
        orch.hostname || "",
        orch.port_map?.grpc?.toString() || "",
      ]
        .join(" ")
        .toLowerCase();

      return searchableText.includes(query);
    });
  }, [searchQuery, orchs]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">
          Loading multiorchestrators...
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
          placeholder="Search by cell, name, hostname, or port..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="max-w-lg"
        />
      </div>

      {filteredOrchs.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12">
          {searchQuery ? (
            <>
              <p className="text-muted-foreground">
                No orchestrators match &quot;{searchQuery}&quot;
              </p>
              <button
                onClick={() => setSearchQuery("")}
                className="mt-2 text-sm text-primary hover:underline"
              >
                Clear search
              </button>
            </>
          ) : (
            <p className="text-muted-foreground">No multiorchestrators found</p>
          )}
        </div>
      ) : (
        <div className="px-4 lg:px-6">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-6">Cell</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Hostname</TableHead>
                <TableHead className="text-right pr-6">gRPC Port</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredOrchs.map((orch, idx) => (
                <TableRow key={orch.id?.name || idx}>
                  <TableCell className="pl-6 font-mono text-xs py-3">
                    {orch.id?.cell || "-"}
                  </TableCell>
                  <TableCell className="font-mono text-xs py-3">
                    {orch.id?.name || "-"}
                  </TableCell>
                  <TableCell className="font-mono text-xs py-3">
                    {orch.hostname || "-"}
                  </TableCell>
                  <TableCell className="text-right pr-6 font-mono text-xs py-3">
                    {orch.port_map?.grpc || "-"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </>
  );
}
