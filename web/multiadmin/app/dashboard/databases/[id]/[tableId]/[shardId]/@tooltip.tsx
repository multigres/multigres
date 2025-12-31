"use client";

import * as React from "react";
import Link from "next/link";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
} from "@/components/ui/tooltip";

type ShardTooltipProps = {
  name: string;
  href: string;
  lagSeconds: number;
  connections: number;
  cpuPercent: number;
  memory: string;
  disk: string;
  children: React.ReactNode;
};

export default function ShardTooltip({
  name,
  href,
  lagSeconds,
  connections,
  cpuPercent,
  memory,
  disk,
  children,
}: ShardTooltipProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex items-center gap-3 cursor-help">
          {children}
        </span>
      </TooltipTrigger>
      <TooltipContent>
        <div className="space-y-2">
          <Link
            href={href}
            className="font-medium underline-offset-4 hover:underline"
          >
            {name}
          </Link>
          <div className="grid grid-cols-2 gap-x-6 gap-y-1 min-w-[220px]">
            <div className="text-muted-foreground">Lag</div>
            <div className="text-right">{lagSeconds}s</div>
            <div className="text-muted-foreground">Connections</div>
            <div className="text-right">{connections}</div>
            <div className="text-muted-foreground">CPU</div>
            <div className="text-right">{cpuPercent}%</div>
            <div className="text-muted-foreground">Memory</div>
            <div className="text-right">{memory}</div>
            <div className="text-muted-foreground">Disk</div>
            <div className="text-right">{disk}</div>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}
