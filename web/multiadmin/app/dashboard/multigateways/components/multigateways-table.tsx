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
import type { MultiGateway } from "@/lib/api/types";

export function MultiGatewaysTable() {
  const api = useApi();
  const [gateways, setGateways] = useState<MultiGateway[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchGateways() {
      try {
        setLoading(true);
        setError(null);

        const { gateways } = await api.getGateways();
        setGateways(gateways || []);
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to fetch multigateways"
        );
      } finally {
        setLoading(false);
      }
    }

    fetchGateways();
  }, [api]);

  const filteredGateways = useMemo(() => {
    if (!searchQuery.trim()) {
      return gateways;
    }

    const query = searchQuery.toLowerCase();
    return gateways.filter((gateway) => {
      const searchableText = [
        gateway.id?.cell || "",
        gateway.id?.name || "",
        gateway.hostname || "",
        gateway.port_map?.grpc?.toString() || "",
        gateway.port_map?.http?.toString() || "",
        gateway.port_map?.postgres?.toString() || "",
      ]
        .join(" ")
        .toLowerCase();

      return searchableText.includes(query);
    });
  }, [searchQuery, gateways]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">
          Loading multigateways...
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

      {filteredGateways.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12">
          {searchQuery ? (
            <>
              <p className="text-muted-foreground">
                No gateways match &quot;{searchQuery}&quot;
              </p>
              <button
                onClick={() => setSearchQuery("")}
                className="mt-2 text-sm text-primary hover:underline"
              >
                Clear search
              </button>
            </>
          ) : (
            <p className="text-muted-foreground">No multigateways found</p>
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
                <TableHead className="text-right">gRPC Port</TableHead>
                <TableHead className="text-right">HTTP Port</TableHead>
                <TableHead className="text-right pr-6">Postgres Port</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredGateways.map((gateway, idx) => (
                <TableRow key={gateway.id?.name || idx}>
                  <TableCell className="pl-6 font-medium py-3">
                    {gateway.id?.cell || "-"}
                  </TableCell>
                  <TableCell className="font-medium py-3">
                    {gateway.id?.name || "-"}
                  </TableCell>
                  <TableCell className="font-mono text-xs py-3">
                    {gateway.hostname || "-"}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs py-3">
                    {gateway.port_map?.grpc || "-"}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs py-3">
                    {gateway.port_map?.http || "-"}
                  </TableCell>
                  <TableCell className="text-right pr-6 font-mono text-xs py-3">
                    {gateway.port_map?.postgres || "-"}
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
