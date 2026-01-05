import { PageLayout } from "@/components/page-layout";

export default function Page() {
  return (
    <PageLayout
      title="Replication"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Replication" },
      ]}
    >
      <div className="px-4 lg:px-6">Replication</div>
    </PageLayout>
  );
}
