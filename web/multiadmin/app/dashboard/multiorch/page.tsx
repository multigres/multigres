import { PageLayout } from "@/components/page-layout";
import { MultiorchTable } from "./components/multiorch-table";

export default function Page() {
  return (
    <PageLayout
      title="Multiorchestrator"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Multiorchestrator" },
      ]}
    >
      <MultiorchTable />
    </PageLayout>
  );
}
