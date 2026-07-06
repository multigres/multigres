import { PageLayout } from "@/components/page-layout";
import { MultipoolersTable } from "./components/multipoolers-table";

export default function Page() {
  return (
    <PageLayout
      title="Multipoolers"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Multipoolers" },
      ]}
    >
      <MultipoolersTable />
    </PageLayout>
  );
}
