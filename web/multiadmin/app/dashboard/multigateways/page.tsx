import { PageLayout } from "@/components/page-layout";

export default function Page() {
  return (
    <PageLayout
      title="MultiGateways"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "MultiGateways" },
      ]}
    >
      <div className="px-4 lg:px-6">MultiGateways</div>
    </PageLayout>
  );
}
