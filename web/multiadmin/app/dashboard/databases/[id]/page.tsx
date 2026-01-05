"use client";
import { PageLayout } from "@/components/page-layout";
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
import { useRouter } from "next/navigation";

type ShardInfo = {
  label: string;
  keyRange: string;
  status: "healthy" | "warning" | "degraded" | "down";
  tablets: number | "unsharded";
};

type TableGroup = {
  name: string;
  shards: ShardInfo[];
};

const MOCK_DATABASES: Record<string, { groups: TableGroup[] }> = {
  "user-service": {
    groups: [
      {
        name: "users",
        shards: [
          {
            label: "-80",
            keyRange: "[-∞, 0x80)",
            status: "healthy",
            tablets: 3,
          },
          {
            label: "80-",
            keyRange: "[0x80, +∞)",
            status: "healthy",
            tablets: 3,
          },
        ],
      },
      {
        name: "orders",
        shards: [
          {
            label: "-40",
            keyRange: "[-∞, 0x40)",
            status: "healthy",
            tablets: 4,
          },
          {
            label: "40-80",
            keyRange: "[0x40, 0x80)",
            status: "warning",
            tablets: 4,
          },
          {
            label: "80-",
            keyRange: "[0x80, +∞)",
            status: "healthy",
            tablets: 4,
          },
        ],
      },
      {
        name: "analytics",
        shards: [
          {
            label: "unsharded",
            keyRange: "unsharded",
            status: "healthy",
            tablets: "unsharded",
          },
        ],
      },
    ],
  },
};

function StatusCell({ status }: { status: ShardInfo["status"] }) {
  const color =
    status === "healthy"
      ? "text-emerald-600"
      : status === "warning"
      ? "text-amber-600"
      : status === "degraded"
      ? "text-yellow-700"
      : "text-red-600";
  return <span className={color}>{status}</span>;
}

export default function Page({ params }: { params: { id: string } }) {
  const id = decodeURIComponent(params.id);
  const router = useRouter();
  const db = MOCK_DATABASES[id] ?? MOCK_DATABASES["user-service"];

  return (
    <PageLayout
      title={id}
      subTitle="Select a table group to view its shards"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Databases", href: "/dashboard/databases" },
        { label: id },
      ]}
    >
      {db.groups.map((group) => (
        <Collapsible key={group.name} className="mb-0" defaultOpen={false}>
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
            </div>
            <div className="flex items-center gap-2">
              <span
                className={`text-sm ${
                  group.shards.some((s) => s.status === "warning")
                    ? "text-amber-600"
                    : "text-muted-foreground"
                }`}
              >
                {group.shards.length}{" "}
                {group.shards.length === 1 ? "shard" : "shards"}
              </span>
            </div>
          </CollapsibleTrigger>
          <CollapsibleContent className="bg-muted/25">
            <Table className="border-t">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-6 w-[220px]">Label</TableHead>
                  <TableHead className="w-[260px]">Key Range</TableHead>
                  <TableHead className="w-[140px]">Status</TableHead>
                  <TableHead className="text-right pr-6 w-[120px]">
                    Tablets
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {group.shards.map((s, i) => (
                  <TableRow
                    key={`${group.name}-${i}`}
                    onClick={() =>
                      router.push(
                        `/dashboard/databases/${encodeURIComponent(
                          id
                        )}/${encodeURIComponent(
                          group.name
                        )}/${encodeURIComponent(String(s.label))}`
                      )
                    }
                    className="cursor-pointer hover:bg-muted/50"
                    role="link"
                    aria-label={`Open shard ${s.label}`}
                  >
                    <TableCell className="pl-6 font-medium w-[220px]">
                      <div className="flex items-center gap-3">
                        <span
                          className={`h-1 w-1 rounded-[4px] ${
                            s.status === "healthy"
                              ? "bg-emerald-600"
                              : s.status === "warning"
                              ? "bg-amber-600"
                              : s.status === "degraded"
                              ? "bg-yellow-700"
                              : "bg-red-600"
                          }`}
                        />
                        <HardDrive
                          strokeWidth={1}
                          size={16}
                          className="text-muted-foreground"
                        />
                        {s.label}
                      </div>
                    </TableCell>
                    <TableCell className="w-[260px]">{s.keyRange}</TableCell>
                    <TableCell className="w-[140px]">
                      <StatusCell status={s.status} />
                    </TableCell>
                    <TableCell className="text-right pr-6 w-[120px]">
                      {s.tablets}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CollapsibleContent>
        </Collapsible>
      ))}
    </PageLayout>
  );
}
