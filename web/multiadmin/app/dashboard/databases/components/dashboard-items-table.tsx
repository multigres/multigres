"use client";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Database } from "lucide-react";
import { useRouter } from "next/navigation";

type DashboardItem = {
  service: string;
  cluster: string;
  tableGroups: number;
  totalShards: number;
  size: string;
  status: "healthy" | "degraded" | "down";
};

const items: DashboardItem[] = [
  {
    service: "user-service",
    cluster: "prod-us-east",
    tableGroups: 3,
    totalShards: 8,
    size: "2.4 GB",
    status: "healthy",
  },
  {
    service: "billing-service",
    cluster: "prod-eu-west",
    tableGroups: 2,
    totalShards: 4,
    size: "1.1 GB",
    status: "healthy",
  },
  {
    service: "orders-service",
    cluster: "staging-us-west",
    tableGroups: 1,
    totalShards: 2,
    size: "350 MB",
    status: "degraded",
  },
];

function StatusCell({ status }: { status: DashboardItem["status"] }) {
  const color =
    status === "healthy"
      ? "text-emerald-600"
      : status === "degraded"
      ? "text-amber-600"
      : "text-red-600";
  return <span className={color}>{status}</span>;
}

export function DashboardItemsTable() {
  const router = useRouter();
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="pl-6">Service</TableHead>
          <TableHead>Cluster</TableHead>
          <TableHead className="text-right">Table Groups</TableHead>
          <TableHead className="text-right">Total Shards</TableHead>
          <TableHead className="text-right">Size</TableHead>
          <TableHead className="pr-6">Status</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((item) => (
          <TableRow
            key={`${item.service}-${item.cluster}`}
            onClick={() =>
              router.push(
                `/dashboard/databases/${encodeURIComponent(item.service)}`
              )
            }
            className="cursor-pointer hover:bg-muted/50"
            role="link"
            aria-label={`Open ${item.service}`}
          >
            <TableCell className="px-6 font-semibold text-foreground py-3">
              <div className="flex items-center gap-2">
                <Database strokeWidth={1} size={16} />
                {item.service}
              </div>
            </TableCell>
            <TableCell>{item.cluster}</TableCell>
            <TableCell className="text-right py-3">
              {item.tableGroups}
            </TableCell>
            <TableCell className="text-right py-3">
              {item.totalShards}
            </TableCell>
            <TableCell className="text-right py-3">{item.size}</TableCell>
            <TableCell className="pr-6 py-3">
              <StatusCell status={item.status} />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
