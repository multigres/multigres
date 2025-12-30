import { PageLayout } from "@/components/page-layout";

export default function Page() {
  return (
    <PageLayout
      title="MultiPoolers"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "MultiPoolers" },
      ]}
    >
      <div className="px-4 lg:px-6">MultiPoolers</div>
    </PageLayout>
  );
}
