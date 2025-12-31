import { PageLayout } from "@/components/page-layout";
import { getTableGroupsWithShards } from "@/lib/db";
import { TableGroupList } from "./table-group-list";

export default async function Page({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const databaseName = decodeURIComponent(id);

  let tableGroups: Awaited<ReturnType<typeof getTableGroupsWithShards>> = [];
  let error: string | null = null;

  try {
    tableGroups = await getTableGroupsWithShards();
  } catch (e) {
    error = e instanceof Error ? e.message : "Failed to fetch table groups";
    tableGroups = [];
  }

  return (
    <PageLayout
      title={databaseName}
      subTitle="Table groups and shards"
      breadcrumbs={[
        { label: "Dashboard", href: "/dashboard" },
        { label: "Databases", href: "/dashboard/databases" },
        { label: databaseName },
      ]}
    >
      {error ? (
        <div className="px-6 py-4 text-destructive">{error}</div>
      ) : tableGroups.length === 0 ? (
        <div className="px-6 py-4 text-muted-foreground">
          No table groups found
        </div>
      ) : (
        <TableGroupList tableGroups={tableGroups} />
      )}
    </PageLayout>
  );
}
