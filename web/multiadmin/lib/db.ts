import { Pool } from "pg";

// PostgreSQL connection pool - connects via multigateway
const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: parseInt(process.env.POSTGRES_PORT || "15434"),
  database: process.env.POSTGRES_DATABASE || "postgres",
  user: process.env.POSTGRES_USER || "postgres",
  password: process.env.POSTGRES_PASSWORD || "postgres",
  ssl: false,
  max: 5,
  idleTimeoutMillis: 30000,
});

export async function query<T>(text: string, params?: unknown[]): Promise<T[]> {
  const client = await pool.connect();
  try {
    const result = await client.query(text, params);
    return result.rows as T[];
  } finally {
    client.release();
  }
}

export interface TableGroupRow {
  oid: number;
  name: string;
  type: string;
}

export interface ShardRow {
  oid: number;
  tablegroup_oid: number;
  shard_name: string;
  key_range_start: string | null;
  key_range_end: string | null;
}

export async function getTableGroups(): Promise<TableGroupRow[]> {
  return query<TableGroupRow>(
    "SELECT oid, name, type FROM multigres.tablegroup",
  );
}

export async function getShards(): Promise<ShardRow[]> {
  return query<ShardRow>(
    "SELECT oid, tablegroup_oid, shard_name, key_range_start, key_range_end FROM multigres.shard",
  );
}

export async function getTableGroupsWithShards() {
  const [tableGroups, shards] = await Promise.all([
    getTableGroups(),
    getShards(),
  ]);

  return tableGroups.map((tg) => ({
    ...tg,
    shards: shards.filter((s) => s.tablegroup_oid === tg.oid),
  }));
}
