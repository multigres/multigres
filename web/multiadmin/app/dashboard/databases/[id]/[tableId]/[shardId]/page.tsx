import { PageLayout } from "@/components/page-layout";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { HardDrive } from "lucide-react";

type NodeInfo = {
  role: "primary" | "replica";
  name: string;
  region: string;
  lagSeconds: number;
  connections: number;
  cpuPercent: number;
  memory: string;
  disk: string;
};

const MOCK_NODES: { primary: NodeInfo; replicas: NodeInfo[] } = {
  primary: {
    role: "primary",
    name: "pg-user-1",
    region: "AWS us-east-1",
    lagSeconds: 0,
    connections: 45,
    cpuPercent: 12,
    memory: "2.1GB",
    disk: "45GB",
  },
  replicas: [
    {
      role: "replica",
      name: "pg-user-replica-1",
      region: "AWS us-east-1",
      lagSeconds: 0,
      connections: 12,
      cpuPercent: 8,
      memory: "1.8GB",
      disk: "45GB",
    },
    {
      role: "replica",
      name: "pg-user-replica-2",
      region: "AWS us-east-1",
      lagSeconds: 0,
      connections: 8,
      cpuPercent: 6,
      memory: "1.6GB",
      disk: "45GB",
    },
  ],
};

export default function Page({
  params,
}: {
  params: { id: string; tableId: string; shardId: string };
}) {
  const { id, tableId, shardId } = params;

  const actions = (
    <div className="flex items-center gap-1">
      <Button variant="outline" size="sm">
        Planned Failover
      </Button>
      <Button variant="outline" size="sm">
        Health Check
      </Button>
      <Button variant="outline" size="sm">
        Cluster Config
      </Button>
      <Button size="sm">Backup</Button>
    </div>
  );

  return (
    <PageLayout
      title={`Shard ${shardId}`}
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Databases", href: "/dashboard/databases" },
        { label: decodeURIComponent(id), href: `/dashboard/databases/${id}` },
        {
          label: decodeURIComponent(tableId),
          href: `/dashboard/databases/${id}/${tableId}`,
        },
        { label: decodeURIComponent(shardId) },
      ]}
      actions={actions}
    >
      <div className="px-6">
        <Card className="mb-6 gap-0 p-0">
          <CardHeader className="flex items-baseline justify-between py-3">
            <CardTitle>Primary</CardTitle>
          </CardHeader>
          <CardContent className="p-0 border-t">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-6 w-[220px]">Name</TableHead>
                  <TableHead className="w-[220px]">Region</TableHead>
                  <TableHead className="w-[120px]">Lag</TableHead>
                  <TableHead className="w-[140px] text-right">
                    Connections
                  </TableHead>
                  <TableHead className="w-[100px] text-right">CPU</TableHead>
                  <TableHead className="w-[120px] text-right">Memory</TableHead>
                  <TableHead className="w-[120px] text-right pr-6">
                    Disk
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow>
                  <TableCell className="pl-6 font-medium w-[220px]">
                    <div className="flex items-center gap-3">
                      <HardDrive
                        strokeWidth={1}
                        size={16}
                        className="text-muted-foreground"
                      />
                      {MOCK_NODES.primary.name}
                    </div>
                  </TableCell>
                  <TableCell className="w-[220px]">
                    {MOCK_NODES.primary.region}
                  </TableCell>
                  <TableCell className="w-[120px]">{`${MOCK_NODES.primary.lagSeconds}s`}</TableCell>
                  <TableCell className="w-[140px] text-right">
                    {MOCK_NODES.primary.connections}
                  </TableCell>
                  <TableCell className="w-[100px] text-right">{`${MOCK_NODES.primary.cpuPercent}%`}</TableCell>
                  <TableCell className="w-[120px] text-right">
                    {MOCK_NODES.primary.memory}
                  </TableCell>
                  <TableCell className="w-[120px] text-right pr-6">
                    {MOCK_NODES.primary.disk}
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card className="p-0 gap-0">
          <CardHeader className="py-3 space-y-0 gap-0">
            <CardTitle>Replicas ({MOCK_NODES.replicas.length})</CardTitle>
          </CardHeader>
          <CardContent className="p-0 border-t">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-6 w-[220px]">Name</TableHead>
                  <TableHead className="w-[220px]">Region</TableHead>
                  <TableHead className="w-[120px]">Lag</TableHead>
                  <TableHead className="w-[140px] text-right">
                    Connections
                  </TableHead>
                  <TableHead className="w-[100px] text-right">CPU</TableHead>
                  <TableHead className="w-[120px] text-right">Memory</TableHead>
                  <TableHead className="w-[120px] text-right pr-6">
                    Disk
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {MOCK_NODES.replicas.map((r) => (
                  <TableRow key={r.name}>
                    <TableCell className="pl-6 font-medium w-[220px]">
                      <div className="flex items-center gap-3">
                        <HardDrive
                          strokeWidth={1}
                          size={16}
                          className="text-muted-foreground"
                        />
                        {r.name}
                      </div>
                    </TableCell>
                    <TableCell className="w-[220px]">{r.region}</TableCell>
                    <TableCell className="w-[120px]">{`${r.lagSeconds}s`}</TableCell>
                    <TableCell className="w-[140px] text-right">
                      {r.connections}
                    </TableCell>
                    <TableCell className="w-[100px] text-right">{`${r.cpuPercent}%`}</TableCell>
                    <TableCell className="w-[120px] text-right">
                      {r.memory}
                    </TableCell>
                    <TableCell className="w-[120px] text-right pr-6">
                      {r.disk}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>
    </PageLayout>
  );
}
