import { PageLayout } from "@/components/page-layout";
import { MultiGatewaysTable } from "./components/multigateways-table";

export default function Page() {
  return (
    <PageLayout
      title="MultiGateways"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "MultiGateways" },
      ]}
    >
      <MultiGatewaysTable />
    </PageLayout>
  );
}
