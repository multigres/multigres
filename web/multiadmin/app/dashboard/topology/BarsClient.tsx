"use client";

import Link from "next/link";
import { useMemo } from "react";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Button } from "@/components/ui/button";

type Column = {
  color: string;
  name: string;
  lagSeconds: number;
  connections: number;
  cpuPercent: number;
  memory: string;
  disk: string;
  href: string | null;
};

function pickColor(): string {
  const r = Math.random();
  if (r < 0.9) return "bg-border"; // success 90%
  if (r < 0.98) return "bg-amber-500"; // warning 8%
  return "bg-red-500"; // error 2%
}

function generateColumns(count: number): Column[] {
  return Array.from({ length: count }, (_, i) => ({
    color: pickColor(),
    name: `shard-${i + 1}`,
    lagSeconds: Math.floor(Math.random() * 3),
    connections: Math.floor(Math.random() * 200),
    cpuPercent: Math.floor(Math.random() * 100),
    memory: `${(Math.random() * 8 + 0.5).toFixed(1)}GB`,
    disk: `${(Math.random() * 200 + 10).toFixed(0)}GB`,
    href: `/dashboard/databases/mock-db/mock-table/${encodeURIComponent(
      `shard-${i + 1}`
    )}`,
  }));
}

export default function BarsClient({
  count = 100,
  shards,
  linkBaseHref,
}: {
  count?: number;
  shards?: Partial<Column>[];
  linkBaseHref?: string;
}) {
  const columns = useMemo(() => {
    const base = generateColumns(count);
    if (!shards || shards.length === 0) return base;
    return base.map((c, i) => {
      const s = shards[i];
      const href =
        s?.href ??
        (linkBaseHref ? `${linkBaseHref}/${encodeURIComponent(c.name)}` : null);
      return {
        ...c,
        ...s,
        href,
      } as Column;
    });
  }, [count, shards, linkBaseHref]);

  return (
    <div className="w-full h-full flex items-center justify-center">
      <div className="w-full h-full flex justify-between">
        {columns.map((col, idx) => (
          <HoverCard key={idx}>
            <HoverCardTrigger asChild>
              <div className={`${col.color} w-[2px] h-full`} />
            </HoverCardTrigger>
            <HoverCardContent className="text-sm">
              {col.href ? (
                <Link href={col.href} className="font-semibold">
                  {col.name}
                </Link>
              ) : (
                <div className="font-semibold">{col.name}</div>
              )}
              <div className="grid grid-cols-2 gap-x-6 gap-y-1 min-w-[220px] mt-2 mb-4">
                <div className="text-muted-foreground">Lag</div>
                <div className="text-right">{col.lagSeconds}s</div>
                <div className="text-muted-foreground">Connections</div>
                <div className="text-right">{col.connections}</div>
                <div className="text-muted-foreground">CPU</div>
                <div className="text-right text-amber-500">
                  {col.cpuPercent}%
                </div>
                <div className="text-muted-foreground">Memory</div>
                <div className="text-right">{col.memory}</div>
                <div className="text-muted-foreground">Disk</div>
                <div className="text-right">{col.disk}</div>
              </div>
              <Button variant="outline" className="w-full block" size="sm">
                View Details
              </Button>
            </HoverCardContent>
          </HoverCard>
        ))}
      </div>
    </div>
  );
}
