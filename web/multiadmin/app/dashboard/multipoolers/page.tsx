import { PageLayout } from "@/components/page-layout";
import { MultiPoolersTable } from "./components/multipoolers-table";

export default function Page() {
  return (
    <PageLayout
      title="MultiPoolers"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "MultiPoolers" },
      ]}
    >
      <MultiPoolersTable />
    </PageLayout>
  );
}
