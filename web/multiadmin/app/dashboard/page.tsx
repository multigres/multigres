import {
  Card,
  CardContent,
  CardTitle,
  CardFooter,
  CardHeader,
  CardDescription,
} from "@/components/ui/card";
import BarsClient from "./topology/BarsClient";
import { PageLayout } from "@/components/page-layout";
import Link from "next/link";

export default function Page() {
  const clusters = [
    {
      name: "prod-us-east",
      role: "Primary",
      tables: 156,
      replicas: 3,
      status: "Healthy",
    },
    {
      name: "dev-cluster-1",
      role: "Development",
      tables: 42,
      replicas: 1,
      status: "Healthy",
    },
  ];

  const activities = [
    {
      title: "High connection count on cluster prod-us-east",
      time: "5 min ago",
    },
    { title: "Backup completed for dev-cluster-1", time: "15 min ago" },
    { title: "Failover test completed successfully", time: "2 hours ago" },
  ];

  return (
    <PageLayout breadcrumbs={[{ label: "Overview" }]}>
      <div className="@container p-6 h-full flex flex-col overflow-hidden">
        <div className="flex items-center gap-2 justify-between mb-6">
          <h1 className="text-4xl font-mono uppercase">Acme Inc</h1>
          <div className="text-4xl font-mono uppercase text-emerald-500">
            /Healty
          </div>
        </div>
        {/* Metrics */}
        <div className="flex-1 mb-6">
          <BarsClient />
        </div>
        <div className="grid grid-cols-2 gap-2 shrink-0">
          <div className="grid grid-cols-2 gap-2">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between gap-2">
                  <CardTitle>Databases</CardTitle>
                  <span className="h-1.5 w-1.5 bg-emerald-500" />
                </div>
                <CardDescription className="text-2xl font-medium text-foreground tabular-nums font-mono">
                  12
                </CardDescription>
              </CardHeader>
              <CardFooter className="text-sm text-muted-foreground">
                Active clusters
              </CardFooter>
            </Card>

            <Card>
              <CardHeader className="has-[data-slot=badge]:grid-cols-[1fr_auto]">
                <div className="flex items-center justify-between gap-2">
                  <CardTitle>Replication Lag</CardTitle>
                  <span className="h-1.5 w-1.5 bg-emerald-500" />
                </div>
                <CardDescription className="text-2xl font-medium text-foreground tabular-nums font-mono">
                  0.2s
                </CardDescription>
              </CardHeader>
              <CardFooter className="text-sm text-muted-foreground">
                Average across primaries
              </CardFooter>
            </Card>

            <Card>
              <CardHeader className="has-[data-slot=badge]:grid-cols-[1fr_auto]">
                <div className="flex items-center justify-between gap-2">
                  <CardTitle>Active Connections</CardTitle>
                  <span className="h-1.5 w-1.5 bg-emerald-500" />
                </div>
                <CardDescription className="text-2xl font-medium text-foreground tabular-nums font-mono">
                  1,247
                </CardDescription>
              </CardHeader>
              <CardFooter className="text-sm text-muted-foreground">
                Across all poolers
              </CardFooter>
            </Card>

            <Card>
              <CardHeader className="has-[data-slot=badge]:grid-cols-[1fr_auto]">
                <div className="flex items-center justify-between gap-2">
                  <CardTitle>Backup Status</CardTitle>
                  <span className="h-1.5 w-1.5 bg-amber-500" />
                </div>
                <CardTitle className="text-2xl font-medium text-foreground tabular-nums font-mono">
                  99.9%
                </CardTitle>
              </CardHeader>
              <CardFooter className="text-sm text-muted-foreground">
                Successful last 30 days
              </CardFooter>
            </Card>
          </div>

          {/* Topology + right column */}
          <Card className="gap-0">
            <CardHeader className="flex items-center justify-between">
              <CardTitle>Recent Activity</CardTitle>
              <Link
                href="#"
                className="text-sm font-medium text-muted-foreground hover:underline"
              >
                View All Activity
              </Link>
            </CardHeader>
            <CardContent className="pt-2">
              <ol className="text-sm relative list-none pl-0">
                {activities.map((a) => (
                  <li
                    key={a.title}
                    className="grid grid-cols-[auto_1fr] items-center gap-3"
                  >
                    <div className="relative w-6 h-full py-6">
                      <div className="absolute top-0 bottom-0 left-1/2 w-px bg-border -translate-x-1/2" />
                      <div className="relative z-10 absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 h-2 w-2 bg-muted-foreground" />
                    </div>
                    <div>
                      <div className="font-medium text-sm">{a.title}</div>
                      <div className="text-sm text-muted-foreground">
                        {a.time}
                      </div>
                    </div>
                  </li>
                ))}
              </ol>
            </CardContent>
          </Card>
        </div>
      </div>
    </PageLayout>
  );
}
