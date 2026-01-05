import { PageLayout } from "@/components/page-layout";
import TopologyClient from "./TopologyClient";

export default function Page() {
  return (
    <PageLayout
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Topology" },
      ]}
    >
      <TopologyClient />
    </PageLayout>
  );
}
