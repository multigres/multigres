"use server";

import { getTableGroups, getShards } from "@/lib/db";

export async function getTableGroupStats() {
  try {
    const [tableGroups, shards] = await Promise.all([
      getTableGroups(),
      getShards(),
    ]);
    return {
      tableGroupCount: tableGroups.length,
      shardCount: shards.length,
      error: null,
    };
  } catch (e) {
    return {
      tableGroupCount: 0,
      shardCount: 0,
      error: e instanceof Error ? e.message : "Failed to fetch stats",
    };
  }
}
