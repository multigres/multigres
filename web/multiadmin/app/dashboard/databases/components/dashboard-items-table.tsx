"use client";

import { useEffect, useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Database, Loader2 } from "lucide-react";
import { useRouter } from "next/navigation";
import { useApi } from "@/lib/api";

type DatabaseInfo = {
  name: string;
  tableGroups: number;
  totalShards: number;
};

export function DashboardItemsTable() {
  const router = useRouter();
  const api = useApi();
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchDatabases() {
      try {
        setLoading(true);
        setError(null);

        // Get all database names
        const { names } = await api.getDatabaseNames();

        // Fetch details for each database
        const dbInfos: DatabaseInfo[] = await Promise.all(
          names.map(async (name) => {
            try {
              const { database } = await api.getDatabase(name);
              const tableGroups = database.tableGroups?.length ?? 0;
              const totalShards =
                database.tableGroups?.reduce(
                  (sum, tg) => sum + (tg.shards?.length ?? 0),
                  0
                ) ?? 0;
              return { name, tableGroups, totalShards };
            } catch {
              // If we can't get details, just show the name
              return { name, tableGroups: 0, totalShards: 0 };
            }
          })
        );

        setDatabases(dbInfos);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch databases");
      } finally {
        setLoading(false);
      }
    }

    fetchDatabases();
  }, [api]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">Loading databases...</span>
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

  if (databases.length === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-muted-foreground">No databases found</p>
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="pl-6">Database</TableHead>
          <TableHead className="text-right">Table Groups</TableHead>
          <TableHead className="text-right">Total Shards</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {databases.map((db) => (
          <TableRow
            key={db.name}
            onClick={() =>
              router.push(`/dashboard/databases/${encodeURIComponent(db.name)}`)
            }
            className="cursor-pointer hover:bg-muted/50"
            role="link"
            aria-label={`Open ${db.name}`}
          >
            <TableCell className="px-6 font-semibold text-foreground py-3">
              <div className="flex items-center gap-2">
                <Database strokeWidth={1} size={16} />
                {db.name}
              </div>
            </TableCell>
            <TableCell className="text-right py-3">{db.tableGroups}</TableCell>
            <TableCell className="text-right py-3">{db.totalShards}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
