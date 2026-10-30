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
  tablegroup_id: number;
  name: string;
  state: string;
}

export interface ShardRow {
  shard_id: number;
  tablegroup_id: number;
  shard_name: string;
  state: string;
}

export async function getTableGroups(): Promise<TableGroupRow[]> {
  return query<TableGroupRow>(
    "SELECT tablegroup_id, name, state FROM multigres.tablegroup",
  );
}

export async function getShards(): Promise<ShardRow[]> {
  return query<ShardRow>(
    "SELECT shard_id, tablegroup_id, shard_name, state FROM multigres.shard",
  );
}

export async function getTableGroupsWithShards() {
  const [tableGroups, shards] = await Promise.all([
    getTableGroups(),
    getShards(),
  ]);

  return tableGroups.map((tg) => ({
    ...tg,
    shards: shards.filter((s) => s.tablegroup_id === tg.tablegroup_id),
  }));
}
