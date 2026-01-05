import { PageLayout } from "@/components/page-layout";

export default function Page() {
  return (
    <PageLayout
      title="Durability"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Durability" },
      ]}
    >
      <div className="px-4 lg:px-6">Durability</div>
    </PageLayout>
  );
}
