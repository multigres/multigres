import { PageLayout } from "@/components/page-layout";

export default function Page() {
  return (
    <PageLayout
      title="Settings"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Settings" },
      ]}
    >
      <div className="px-4 lg:px-6">Settings</div>
    </PageLayout>
  );
}
