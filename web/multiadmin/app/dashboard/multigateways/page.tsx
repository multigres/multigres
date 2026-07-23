import { PageLayout } from "@/components/page-layout";
import { MultigatewaysTable } from "./components/multigateways-table";

export default function Page() {
  return (
    <PageLayout
      title="Multigateways"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Multigateways" },
      ]}
    >
      <MultigatewaysTable />
    </PageLayout>
  );
}
