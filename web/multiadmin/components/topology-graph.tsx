"use client";

import React, { useMemo } from "react";
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  type Node,
  type Edge,
} from "reactflow";
import "reactflow/dist/style.css";
import { cn } from "@/lib/utils";

function StatusBadge({ status }: { status: string }) {
  const isActive = status.toLowerCase() === "active";
  return (
    <div className={cn(isActive ? "text-green-500" : "text-red-500")}>
      {status}
    </div>
  );
}

export function TopologyGraph({ heightClass }: { heightClass?: string }) {
  const nodes: Node[] = useMemo(() => {
    return [
      {
        id: "lb",
        position: { x: 600, y: 0 },
        data: {
          label: (
            <div className="text-left text-sm p-3">
              <div className="font-medium">Load Balancer</div>
              <div className="text-xs text-muted-foreground">
                MultiGateway Layer
              </div>
            </div>
          ),
        },
        type: "default",
      },

      // Gateways
      {
        id: "gw-1",
        position: { x: 300, y: 150 },
        data: {
          label: (
            <div className="text-left text-sm p-3">
              <div className="font-medium">gateway-1</div>
              <div className="mt-1">
                <StatusBadge status="Active" />
              </div>
            </div>
          ),
        },
      },
      {
        id: "gw-2",
        position: { x: 500, y: 150 },
        data: {
          label: (
            <div className="text-left text-sm p-3">
              <div className="font-medium">gateway-2</div>
              <div className="mt-1">
                <StatusBadge status="Active" />
              </div>
            </div>
          ),
        },
      },
      {
        id: "gw-3",
        position: { x: 700, y: 150 },
        data: {
          label: (
            <div className="text-left text-sm p-3">
              <div className="font-medium">gateway-3</div>
              <div className="mt-1">
                <StatusBadge status="Active" />
              </div>
            </div>
          ),
        },
      },
      {
        id: "gw-4",
        position: { x: 900, y: 150 },
        data: {
          label: (
            <div className="text-left text-sm p-3">
              <div className="font-medium">gateway-4</div>
              <div className="mt-1">
                <StatusBadge status="Active" />
              </div>
            </div>
          ),
        },
      },

      // Clusters
      {
        id: "cluster-east",
        position: { x: 450, y: 320 },
        data: {
          label: (
            <div className="text-left text-xs">
              <div className="bg-sidebar p-3 border-b">
                <div className="font-semibold">prod-us-east</div>
                <div className="text-muted-foreground">Primary</div>
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                <div className="text-muted-foreground">Databases</div>
                <div className="text-right">5</div>
                <div className="text-muted-foreground">Replicas</div>
                <div className="text-right">3</div>
                <div className="text-muted-foreground">Connections</div>
                <div className="text-right">456</div>
                <div className="text-muted-foreground">Lag</div>
                <div className="text-right">0.1s</div>
              </div>
            </div>
          ),
        },
      },
      {
        id: "cluster-west",
        position: { x: 650, y: 320 },
        data: {
          label: (
            <div className="text-left text-xs">
              <div className="bg-sidebar p-3 border-b">
                <div className="font-semibold">prod-us-west</div>
                <div className="text-muted-foreground">Secondary</div>
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                <div className="text-muted-foreground">Databases</div>
                <div className="text-right">5</div>
                <div className="text-muted-foreground">Replicas</div>
                <div className="text-right">2</div>
                <div className="text-muted-foreground">Connections</div>
                <div className="text-right">289</div>
                <div className="text-muted-foreground">Lag</div>
                <div className="text-right">0.3s</div>
              </div>
            </div>
          ),
        },
      },
      {
        id: "cluster-dev",
        position: { x: 850, y: 320 },
        data: {
          label: (
            <div className="text-left text-xs">
              <div className="bg-sidebar p-3 border-b">
                <div className="font-semibold">dev-cluster-1</div>
                <div className="text-muted-foreground">Development</div>
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 p-3">
                <div className="text-muted-foreground">Databases</div>
                <div className="text-right">2</div>
                <div className="text-muted-foreground">Replicas</div>
                <div className="text-right">1</div>
                <div className="text-muted-foreground">Connections</div>
                <div className="text-right">45</div>
                <div className="text-muted-foreground">Lag</div>
                <div className="text-right">1.2s</div>
              </div>
            </div>
          ),
        },
      },
    ];
  }, []);

  const edges: Edge[] = useMemo(() => {
    const gatewayIds = ["gw-1", "gw-2", "gw-3", "gw-4"];
    const clusterIds = ["cluster-east", "cluster-west", "cluster-dev"];

    const fromLb = gatewayIds.map((gwId) => ({
      id: `lb-${gwId}`,
      source: "lb",
      target: gwId,
    }));
    const fanout = gatewayIds.flatMap((gwId) =>
      clusterIds.map((cid) => ({
        id: `${gwId}-${cid}`,
        source: gwId,
        target: cid,
      }))
    );

    return [...fromLb, ...fanout];
  }, []);

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
