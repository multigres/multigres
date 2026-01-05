import { PageLayout } from "@/components/page-layout";
import { DashboardItemsTable } from "./components/dashboard-items-table";

export default function Page() {
  return (
    <PageLayout
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Databases" },
      ]}
    >
      <DashboardItemsTable />
    </PageLayout>
  );
}
