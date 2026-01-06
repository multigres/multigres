"use client";

import React, { useEffect, useState, useMemo, useCallback, useRef } from "react";
import "./topology-graph.css";
import ReactFlow, {
  Background,
  BackgroundVariant,
  type Node,
  type Edge,
} from "reactflow";
import "reactflow/dist/style.css";
import { cn } from "@/lib/utils";
import { useApi } from "@/lib/api";
import type { MultiGateway, MultiPoolerWithStatus, ID } from "@/lib/api";
import { Loader2 } from "lucide-react";

// Utility functions for smart data comparison
function getStableId(id?: ID): string {
  return id ? `${id.component}:${id.cell}:${id.name}` : '';
}

// Helper function to format WAL position (LSN)
function formatWalPosition(pooler: MultiPoolerWithStatus): string {
  if (!pooler.status) return "N/A";

  // For primary: show current LSN from primary_status
  if (pooler.type === "PRIMARY" && pooler.status.primary_status?.lsn) {
    return pooler.status.primary_status.lsn;
  }

  // For replica: show last replay LSN from replication_status
  if (pooler.type === "REPLICA" && pooler.status.replication_status?.last_replay_lsn) {
    return pooler.status.replication_status.last_replay_lsn;
  }

  // Fallback to wal_position if available
  if (pooler.status.wal_position) {
    return pooler.status.wal_position;
  }

  return "N/A";
}

// Helper function to check if a replica is connected to the primary
function isReplicaConnected(replica: MultiPoolerWithStatus, primary: MultiPoolerWithStatus): boolean {
  if (!primary.status?.primary_status?.connected_followers || !replica.id) {
    return false;
  }

  return primary.status.primary_status.connected_followers.some(
    follower => follower.cell === replica.id?.cell && follower.name === replica.id?.name
  );
}

// Helper function to count connected replicas for a primary
function countConnectedReplicas(primary: MultiPoolerWithStatus, replicas: MultiPoolerWithStatus[]): number {
  if (!primary.status?.primary_status?.connected_followers) {
    return 0;
  }

  return replicas.filter(replica => isReplicaConnected(replica, primary)).length;
}

// Helper function to check if primary has valid status
function hasPrimaryStatus(primary: MultiPoolerWithStatus): boolean {
  // Check if we have status at all (gRPC call succeeded)
  if (!primary.status) {
    return false;
  }

  // Check if postgres is running and we have primary status
  if (!primary.status.postgres_running || !primary.status.primary_status) {
    return false;
  }

  return true;
}

// Helper function to calculate lag from timestamp
function calculateLagFromTimestamp(timestamp: string): string {
  try {
    const replayTime = new Date(timestamp);
    const now = new Date();
    const lagMs = now.getTime() - replayTime.getTime();

    // Show in milliseconds if < 1000ms, otherwise show in seconds
    if (lagMs < 1000) {
      return `${lagMs}ms`;
    } else {
      return `${(lagMs / 1000).toFixed(1)}s`;
    }
  } catch (err) {
    console.error("Failed to parse timestamp:", err);
    return "N/A";
  }
}

// Helper function to format replication lag
function formatReplicationLag(pooler: MultiPoolerWithStatus): string {
  if (!pooler.status) return "N/A";

  // Only show lag for replicas
  if (pooler.type === "REPLICA" && pooler.status.replication_status?.last_xact_replay_timestamp) {
    return calculateLagFromTimestamp(pooler.status.replication_status.last_xact_replay_timestamp);
  }

  return "N/A";
}

function hasDataChanged<T extends { id?: ID }>(prev: T[], next: T[]): boolean {
  // Check topology structure change (nodes added/removed)
  if (prev.length !== next.length) return true;

  const prevIds = new Set(prev.map(item => getStableId(item.id)));
  const nextIds = new Set(next.map(item => getStableId(item.id)));

  if (prevIds.size !== nextIds.size) return true;
  for (const id of nextIds) {
    if (!prevIds.has(id)) return true;
  }

  // Check field changes
  const prevMap = new Map(prev.map(item => [getStableId(item.id), item]));
  for (const item of next) {
    const id = getStableId(item.id);
    const prevItem = prevMap.get(id);
    if (!prevItem) continue;
    if (JSON.stringify(prevItem) !== JSON.stringify(item)) {
      return true;
    }
  }

  return false;
}

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

function isPrimary(pooler: MultiPoolerWithStatus): boolean {
  return pooler.type === "PRIMARY";
}

export function TopologyGraph({ heightClass: _heightClass }: { heightClass?: string }) {
  const api = useApi();
  const [gateways, setGateways] = useState<MultiGateway[]>([]);
  const [poolers, setPoolers] = useState<MultiPoolerWithStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const prevDataRef = useRef({ gateways: [] as MultiGateway[], poolers: [] as MultiPoolerWithStatus[] });

  const fetchData = useCallback(async (isInitial = false) => {
    try {
      if (isInitial) {
        setLoading(true);
      }
      setError(null);

      const [gatewaysRes, poolersRes] = await Promise.all([
        api.getGateways(),
        api.getPoolers(),
      ]);

      const newGateways = gatewaysRes.gateways || [];
      const basePoolers = poolersRes.poolers || [];

      // Fetch status for each pooler in parallel
      const poolerStatusPromises = basePoolers.map(async (pooler) => {
        if (!pooler.id) return pooler;

        try {
          const statusRes = await api.getPoolerStatus(pooler.id);
          return { ...pooler, status: statusRes.status };
        } catch (err) {
          console.error(`Failed to fetch status for pooler ${pooler.id.name}:`, err);
          return pooler; // Return pooler without status if fetch fails
        }
      });

      const newPoolers = await Promise.all(poolerStatusPromises);

      // Smart comparison - only update if data actually changed
      const gatewaysChanged = hasDataChanged(prevDataRef.current.gateways, newGateways);
      const poolersChanged = hasDataChanged(prevDataRef.current.poolers, newPoolers);

      if (gatewaysChanged || poolersChanged) {
        setGateways(newGateways);
        setPoolers(newPoolers);
        prevDataRef.current = { gateways: newGateways, poolers: newPoolers };
      }
    } catch (err) {
      // On initial fetch, show error
      if (isInitial) {
        setError(err instanceof Error ? err.message : "Failed to fetch topology");
      } else {
        // On polling errors, log but don't disrupt UI
        console.error("Polling error:", err);
      }
    } finally {
      if (isInitial) {
        setLoading(false);
      }
    }
  }, [api]);

  useEffect(() => {
    // Initial fetch
    fetchData(true);

    // Get poll interval from env or use default (500ms)
    const pollInterval = parseInt(
      process.env.NEXT_PUBLIC_TOPOLOGY_POLL_INTERVAL || '500',
      10
    );

    // Start polling
    const intervalId = setInterval(() => {
      fetchData(false);
    }, pollInterval);

    // Cleanup on unmount
    return () => clearInterval(intervalId);
  }, [fetchData]);

  const { nodes, edges } = useMemo(() => {
    if (gateways.length === 0 && poolers.length === 0) {
      return { nodes: [], edges: [] };
    }

    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Layout constants
    const GATEWAY_Y = 0;
    const PRIMARY_Y = 200;
    const REPLICA_Y = 450;
    const NODE_WIDTH = 200;
    const NODE_GAP = 100;

    // Create gateway nodes
    const gatewayStartX = 100;
    gateways.forEach((gw, index) => {
      const id = `gw-${gw.id?.name || index}`;
      nodes.push({
        id,
        position: { x: gatewayStartX + index * (NODE_WIDTH + NODE_GAP), y: GATEWAY_Y },
        data: {
          label: (
            <div className="text-left text-xs">
              <div className="bg-sidebar p-3 border-b">
                <div className="text-muted-foreground text-[10px] uppercase tracking-wide mb-1">Gateway</div>
                <div className="font-semibold">{gw.id?.name || `gateway-${index + 1}`}</div>
                <div className="mt-1">
                  <StatusBadge status="Active" />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                <div className="text-muted-foreground">Cell</div>
                <div className="text-right">{gw.id?.cell || "unknown"}</div>
              </div>
            </div>
          ),
        },
      });
    });

    // Group poolers by database/table_group/shard
    const poolerGroups = new Map<string, { primary?: MultiPoolerWithStatus; replicas: MultiPoolerWithStatus[] }>();

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
        const primary = group.primary; // Store for type narrowing in nested scopes
        const primaryId = `pooler-${primary.id?.name || `primary-${groupIndex}`}`;
        const hasValidStatus = hasPrimaryStatus(primary);
        const hasConnectedReplicas = countConnectedReplicas(primary, group.replicas) > 0;

        // Show red border if: no valid status OR (has replicas but none connected)
        const showDisconnected = !hasValidStatus || (!hasConnectedReplicas && group.replicas.length > 0);

        nodes.push({
          id: primaryId,
          position: { x: groupCenterX + NODE_WIDTH / 2, y: PRIMARY_Y },
          className: showDisconnected ? 'disconnected-node' : '',
          data: {
            label: (
              <div className="text-left text-xs">
                <div className="bg-sidebar p-3 border-b">
                  <div className="text-muted-foreground text-[10px] uppercase tracking-wide mb-1">Pooler</div>
                  <div className="flex items-center gap-2">
                    <span className="font-semibold">
                      {primary.id?.name || "primary"}
                    </span>
                    <PoolerTypeBadge type="primary" />
                  </div>
                  <div className="text-muted-foreground">
                    {database} / {tableGroup} / {shard}
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                  <div className="text-muted-foreground">Cell</div>
                  <div className="text-right">{primary.id?.cell || "unknown"}</div>
                  <div className="text-muted-foreground">WAL Pos</div>
                  <div className="text-right text-xs">{formatWalPosition(primary)}</div>
                  <div className="text-muted-foreground">Lag</div>
                  <div className="text-right">{formatReplicationLag(primary)}</div>
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
          const isConnected = isReplicaConnected(replica, primary);

          nodes.push({
            id: replicaId,
            position: { x: replicaX, y: REPLICA_Y },
            className: !isConnected ? 'disconnected-node' : '',
            data: {
              label: (
                <div className="text-left text-xs">
                  <div className="bg-sidebar p-3 border-b">
                    <div className="text-muted-foreground text-[10px] uppercase tracking-wide mb-1">Pooler</div>
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
                    <div className="text-muted-foreground">WAL Pos</div>
                    <div className="text-right text-xs">{formatWalPosition(replica)}</div>
                    <div className="text-muted-foreground">Lag</div>
                    <div className="text-right">{formatReplicationLag(replica)}</div>
                  </div>
                </div>
              ),
            },
          });

          // Connect primary to replica (only if connected)
          if (isConnected) {
            edges.push({
              id: `${primaryId}-${replicaId}`,
              source: primaryId,
              target: replicaId,
              className: 'connected-edge',
              animated: true,
            });
          }
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
