"use client";

import TopologyGraph from "@/components/topology-graph";

export default function TopologyClient({
  heightClass,
}: {
  heightClass?: string;
}) {
  return <TopologyGraph heightClass={heightClass} />;
}
