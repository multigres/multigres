import { PageLayout } from "@/components/page-layout";

export default function Page() {
  return (
    <PageLayout
      title="Backup & Restore"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Backup & Restore" },
      ]}
    >
      <div className="px-4 lg:px-6">Backup &amp; Restore</div>
    </PageLayout>
  );
}
