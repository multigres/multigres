"use client";

import React, { useEffect, useState, useMemo } from "react";
import ReactFlow, {
  Background,
  BackgroundVariant,
  type Node,
  type Edge,
} from "reactflow";
import "reactflow/dist/style.css";
import { cn } from "@/lib/utils";
import { useApi } from "@/lib/api";
import type { MultiGateway, MultiPooler } from "@/lib/api";
import { Loader2 } from "lucide-react";

function StatusBadge({ status }: { status: string }) {
  const isActive = status.toLowerCase() === "active";
  return (
    <div className={cn(isActive ? "text-green-500" : "text-red-500")}>
      {status}
    </div>
  );
}

function PoolerTypeBadge({ type }: { type: "primary" | "replica" }) {
  return (
    <span
      className={cn(
        "text-xs px-1.5 py-0.5 rounded",
        type === "primary"
          ? "bg-blue-500/20 text-blue-400"
          : "bg-purple-500/20 text-purple-400"
      )}
    >
      {type}
    </span>
  );
}

function isPrimary(pooler: MultiPooler): boolean {
  return pooler.type === "PRIMARY";
}

export function TopologyGraph({ heightClass }: { heightClass?: string }) {
  const api = useApi();
  const [gateways, setGateways] = useState<MultiGateway[]>([]);
  const [poolers, setPoolers] = useState<MultiPooler[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true);
        setError(null);

        const [gatewaysRes, poolersRes] = await Promise.all([
          api.getGateways(),
          api.getPoolers(),
        ]);

        setGateways(gatewaysRes.gateways || []);
        setPoolers(poolersRes.poolers || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch topology");
      } finally {
        setLoading(false);
      }
    }

    fetchData();
  }, [api]);

  const { nodes, edges } = useMemo(() => {
    if (gateways.length === 0 && poolers.length === 0) {
      return { nodes: [], edges: [] };
    }

    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Layout constants
    const GATEWAY_Y = 0;
    const PRIMARY_Y = 150;
    const REPLICA_Y = 320;
    const NODE_WIDTH = 200;
    const NODE_GAP = 50;

    // Create gateway nodes
    const gatewayStartX = 100;
    gateways.forEach((gw, index) => {
      const id = `gw-${gw.id?.name || index}`;
      nodes.push({
        id,
        position: { x: gatewayStartX + index * (NODE_WIDTH + NODE_GAP), y: GATEWAY_Y },
        data: {
          label: (
            <div className="text-left text-sm p-3">
              <div className="font-medium">{gw.id?.name || `gateway-${index + 1}`}</div>
              <div className="text-xs text-muted-foreground">{gw.id?.cell || "unknown"}</div>
              <div className="mt-1">
                <StatusBadge status="Active" />
              </div>
            </div>
          ),
        },
      });
    });

    // Group poolers by database/table_group/shard
    const poolerGroups = new Map<string, { primary?: MultiPooler; replicas: MultiPooler[] }>();

    poolers.forEach((pooler) => {
      const key = `${pooler.database || ""}/${pooler.table_group || "default"}/${pooler.shard || "0-"}`;
      if (!poolerGroups.has(key)) {
        poolerGroups.set(key, { replicas: [] });
      }
      const group = poolerGroups.get(key)!;
      if (isPrimary(pooler)) {
        group.primary = pooler;
      } else {
        group.replicas.push(pooler);
      }
    });

    // Create pooler nodes for each group
    let groupIndex = 0;
    poolerGroups.forEach((group, key) => {
      const [database, tableGroup, shard] = key.split("/");
      const groupCenterX = gatewayStartX + groupIndex * (NODE_WIDTH * 2 + NODE_GAP * 2);

      // Primary node
      if (group.primary) {
        const primaryId = `pooler-${group.primary.id?.name || `primary-${groupIndex}`}`;
        nodes.push({
          id: primaryId,
          position: { x: groupCenterX + NODE_WIDTH / 2, y: PRIMARY_Y },
          data: {
            label: (
              <div className="text-left text-xs">
                <div className="bg-sidebar p-3 border-b">
                  <div className="flex items-center gap-2">
                    <span className="font-semibold">
                      {group.primary.id?.name || "primary"}
                    </span>
                    <PoolerTypeBadge type="primary" />
                  </div>
                  <div className="text-muted-foreground">
                    {database} / {tableGroup} / {shard}
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                  <div className="text-muted-foreground">Cell</div>
                  <div className="text-right">{group.primary.id?.cell || "unknown"}</div>
                </div>
              </div>
            ),
          },
        });

        // Connect all gateways to this primary
        gateways.forEach((gw, gwIndex) => {
          const gwId = `gw-${gw.id?.name || gwIndex}`;
          edges.push({
            id: `${gwId}-${primaryId}`,
            source: gwId,
            target: primaryId,
          });
        });

        // Replica nodes
        group.replicas.forEach((replica, replicaIndex) => {
          const replicaId = `pooler-${replica.id?.name || `replica-${groupIndex}-${replicaIndex}`}`;
          const replicaX = groupCenterX + replicaIndex * (NODE_WIDTH + NODE_GAP);

          nodes.push({
            id: replicaId,
            position: { x: replicaX, y: REPLICA_Y },
            data: {
              label: (
                <div className="text-left text-xs">
                  <div className="bg-sidebar p-3 border-b">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold">
                        {replica.id?.name || `replica-${replicaIndex + 1}`}
                      </span>
                      <PoolerTypeBadge type="replica" />
                    </div>
                    <div className="text-muted-foreground">
                      {database} / {tableGroup} / {shard}
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                    <div className="text-muted-foreground">Cell</div>
                    <div className="text-right">{replica.id?.cell || "unknown"}</div>
                  </div>
                </div>
              ),
            },
          });

          // Connect primary to replica
          edges.push({
            id: `${primaryId}-${replicaId}`,
            source: primaryId,
            target: replicaId,
            style: { strokeDasharray: "5,5" },
            label: "replication",
          });
        });
      }

      groupIndex++;
    });

    return { nodes, edges };
  }, [gateways, poolers]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">Loading topology...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-destructive">{error}</p>
      </div>
    );
  }

  if (nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-muted-foreground">No topology data available</p>
      </div>
    );
  }

  return (
    <div className={cn("w-full h-full font-mono")}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitViewOptions={{ padding: 0.4 }}
        fitView
        className="bg-background"
      >
        <Background
          color="var(--foreground)"
          className="opacity-25"
          variant={BackgroundVariant.Dots}
          gap={16}
        />
      </ReactFlow>
    </div>
  );
}

export default TopologyGraph;
