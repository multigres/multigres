import { PageLayout } from "@/components/page-layout";
import { MultiOrchTable } from "./components/multiorch-table";

export default function Page() {
  return (
    <PageLayout
      title="MultiOrchestrator"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "MultiOrchestrator" },
      ]}
    >
      <MultiOrchTable />
    </PageLayout>
  );
}
