import { PageLayout } from "@/components/page-layout";
import TopologyClient from "./TopologyClient";

export default function Page() {
  const connectionSteps = [
    { n: 1, title: "Client Application", subtitle: "1,247 connections" },
    { n: 2, title: "MultiGateway", subtitle: "1,247 connections" },
    { n: 3, title: "MultiPooler", subtitle: "156 connections" },
    { n: 4, title: "PostgreSQL Primary", subtitle: "45 connections" },
    { n: 5, title: "PostgreSQL Replicas", subtitle: "111 connections" },
  ];
  return (
    <PageLayout
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Topology" },
      ]}
    >
      <div className="flex w-full h-full">
        <div className="flex-1 min-w-0">
          <TopologyClient />
        </div>
        <aside className="w-80 lg:w-96 border-l p-6 space-y-6">
          <section>
            <h2 className="font-semibold">Connection Flow</h2>

            <ol className="mt-3 text-sm relative list-none pl-0">
              <div className="absolute left-3 top-0 bottom-0 w-px bg-border" />
              {connectionSteps.map((step) => (
                <li
                  key={step.n}
                  className="grid grid-cols-[auto_1fr] items-center gap-3 py-2"
                >
                  <div className="relative w-6">
                    <div className="relative z-10 inline-flex h-6 w-6 items-center justify-center rounded-sm border bg-background text-sm font-medium text-foreground bg-muted">
                      {step.n}
                    </div>
                  </div>
                  <div>
                    <div className="font-medium text-sm">{step.title}</div>
                    <div className="text-sm text-muted-foreground">
                      {step.subtitle}
                    </div>
                  </div>
                </li>
              ))}
            </ol>
          </section>

          <section>
            <h2 className="font-semibold">Replication Flow</h2>
            <div className="mt-3 text-sm border rounded">
              <div className="py-3 px-4 border-b bg-muted">
                <div className="font-medium">Primary → Replicas</div>
                <div className="text-sm text-muted-foreground">
                  Synchronous replication with 0.1s average lag
                </div>
              </div>
              <div className="py-3 px-4 border-b bg-muted">
                <div className="font-medium">Cross-Region Sync</div>
                <div className="text-sm text-muted-foreground">
                  Asynchronous backup to us-west region
                </div>
              </div>
              <div className="py-3 px-4 bg-muted">
                <div className="font-medium">Development Sync</div>
                <div className="text-sm text-muted-foreground">
                  Periodic sync from production (1.2s lag)
                </div>
              </div>
            </div>
          </section>
        </aside>
      </div>
    </PageLayout>
  );
}
