"use client";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronRight, HardDrive, Table2 } from "lucide-react";
import type { ShardRow, TableGroupRow } from "@/lib/db";

type TableGroupWithShards = TableGroupRow & {
  shards: ShardRow[];
};

export function TableGroupList({
  tableGroups,
}: {
  tableGroups: TableGroupWithShards[];
}) {
  return (
    <>
      {tableGroups.map((group) => (
        <Collapsible key={group.oid} className="mb-0" defaultOpen={true}>
          <CollapsibleTrigger className="group flex w-full items-baseline justify-between px-6 border-t py-3 text-left cursor-pointer">
            <div className="flex items-center gap-3">
              <ChevronRight
                size={16}
                className="text-muted-foreground transition-transform group-data-[state=open]:rotate-90"
              />
              <Table2
                strokeWidth={1}
                size={16}
                className="text-muted-foreground"
              />
              <h2 className="text-base font-semibold">{group.name}</h2>
              <span className="text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded">
                {group.type}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">
                {group.shards.length}{" "}
                {group.shards.length === 1 ? "shard" : "shards"}
              </span>
            </div>
          </CollapsibleTrigger>
          <CollapsibleContent className="bg-muted/25">
            <Table className="border-t">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-6 w-[220px]">Shard</TableHead>
                  <TableHead>Key Range</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {group.shards.map((shard) => (
                  <TableRow key={shard.oid}>
                    <TableCell className="pl-6 font-medium w-[220px]">
                      <div className="flex items-center gap-3">
                        <span className="h-1 w-1 rounded-[4px] bg-emerald-600" />
                        <HardDrive
                          strokeWidth={1}
                          size={16}
                          className="text-muted-foreground"
                        />
                        {shard.shard_name}
                      </div>
                    </TableCell>
                    <TableCell>{shard.shard_name}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CollapsibleContent>
        </Collapsible>
      ))}
    </>
  );
}
