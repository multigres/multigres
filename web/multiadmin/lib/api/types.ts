// Types matching proto/multiadminservice.proto and proto/clustermetadata.proto

// Cell represents a cell in the cluster topology
export interface Cell {
  name: string;
}

// Database represents a database in the cluster
export interface Database {
  name: string;
  tableGroups?: TableGroup[];
  // Additional fields from clustermetadata.Database as needed
}

export interface TableGroup {
  name: string;
  shards?: Shard[];
}

export interface Shard {
  name: string;
  keyRange?: string;
}

// ID from clustermetadata.proto
export interface ID {
  component: string;
  cell: string;
  name: string;
}

export interface PortMap {
  grpc: number;
  http: number;
  postgres: number;
}

// MultiGateway from clustermetadata.proto
export interface MultiGateway {
  id?: ID;
  hostname?: string;
  port_map?: PortMap;
}

// MultiPooler from clustermetadata.proto
export interface MultiPooler {
  id?: ID;
  database?: string;
  table_group?: string;
  shard?: string;
  key_range?: string | null;
  type?: PoolerType;
  serving_status?: string;
  hostname?: string;
  port_map?: PortMap;
}

// Enriched pooler with status information
export interface MultiPoolerWithStatus extends MultiPooler {
  status?: PoolerStatus;
}

export type PoolerType = "PRIMARY" | "REPLICA";

// MultiOrch from clustermetadata.proto
export interface MultiOrch {
  id?: ID;
  hostname?: string;
  port_map?: PortMap;
}

// Job types and status
export type JobType = "UNKNOWN" | "BACKUP" | "RESTORE";
export type JobStatus =
  | "UNKNOWN"
  | "PENDING"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED";
export type BackupStatus = "UNKNOWN" | "INCOMPLETE" | "COMPLETE" | "FAILED";

// BackupInfo from GetBackups
export interface BackupInfo {
  backupId: string;
  database: string;
  tableGroup: string;
  shard: string;
  type: string;
  status: BackupStatus;
  backupTime?: string;
  backupSizeBytes?: number;
  multipoolerServiceId?: string;
  poolerType?: PoolerType;
}

// Request/Response types

export interface GetCellNamesResponse {
  names: string[];
}

export interface GetCellResponse {
  cell: Cell;
}

export interface GetDatabaseNamesResponse {
  names: string[];
}

export interface GetDatabaseResponse {
  database: Database;
}

export interface GetGatewaysResponse {
  gateways: MultiGateway[];
}

export interface GetPoolersResponse {
  poolers: MultiPooler[];
}

export interface GetOrchsResponse {
  orchs: MultiOrch[];
}

export interface BackupRequest {
  database: string;
  tableGroup: string;
  shard: string;
  type: "full" | "differential" | "incremental";
  forcePrimary?: boolean;
}

export interface BackupResponse {
  jobId: string;
}

export interface RestoreFromBackupRequest {
  database: string;
  tableGroup: string;
  shard: string;
  backupId?: string;
  poolerId: ID;
}

export interface RestoreFromBackupResponse {
  jobId: string;
}

export interface GetBackupJobStatusRequest {
  jobId: string;
  database?: string;
  tableGroup?: string;
  shard?: string;
}

export interface GetBackupJobStatusResponse {
  jobId: string;
  jobType: JobType;
  status: JobStatus;
  errorMessage?: string;
  database: string;
  tableGroup: string;
  shard: string;
  backupType?: string;
  requestedBackupId?: string;
  backupId?: string;
}

export interface GetBackupsRequest {
  database?: string;
  tableGroup?: string;
  shard?: string;
  limit?: number;
}

export interface GetBackupsResponse {
  backups: BackupInfo[];
}

// Pooler Status types

export interface GetPoolerStatusRequest {
  poolerId: ID;
}

export interface GetPoolerStatusResponse {
  status: PoolerStatus;
}

export interface PoolerStatus {
  pooler_type?: PoolerType;
  primary_status?: PrimaryStatus;
  replication_status?: StandbyReplicationStatus;
  is_initialized?: boolean;
  has_data_directory?: boolean;
  postgres_running?: boolean;
  postgres_role?: string;
  wal_position?: string;
  consensus_term?: string;
  shard_id?: string;
}

export interface PrimaryStatus {
  lsn?: string;
  ready?: boolean;
  connected_followers?: ID[];
  sync_replication_config?: SyncReplicationConfig;
}

export interface SyncReplicationConfig {
  synchronous_commit?: string;
  synchronous_method?: string;
  num_sync?: number;
  standby_ids?: ID[];
}

export interface StandbyReplicationStatus {
  last_replay_lsn?: string;
  last_receive_lsn?: string;
  is_wal_replay_paused?: boolean;
  wal_replay_pause_state?: string;
  lag?: string; // Duration as string (e.g., "5s")
  last_xact_replay_timestamp?: string;
}

// Gateway diagnostics types — match proto/multigatewaymanagerdata.proto.
//
// Durations come over the wire as JSON strings ending in "s" (e.g. "0.012s")
// per protojson defaults; trend slices are oldest-to-newest.

export interface QueryStatSnapshot {
  fingerprint: string;
  normalized_sql: string;
  calls: string; // uint64 → string in protojson
  errors: string;
  total_duration?: string;
  average_duration?: string;
  min_duration?: string;
  max_duration?: string;
  p50_duration?: string;
  p99_duration?: string;
  total_rows: string;
  last_seen?: string;
  sample_interval?: string;
  call_rate_trends?: number[];
  total_time_ms_trends?: number[];
  p50_ms_trends?: number[];
  p99_ms_trends?: number[];
  rows_rate_trends?: number[];
}

export interface QueryRegistrySnapshot {
  queries?: QueryStatSnapshot[];
  tracked_fingerprints: number;
}

export interface ConsolidatorPreparedStatement {
  name: string;
  query: string;
  refs: number;
}

export interface ConsolidatorStats {
  unique_statements: number;
  total_references: number;
  connection_count: number;
  prepared_statements?: ConsolidatorPreparedStatement[];
}

export interface GetGatewayQueriesRequest {
  gatewayId: ID;
  limit?: number;
  minCalls?: number;
}

export interface GetGatewayQueriesResponse {
  snapshot: QueryRegistrySnapshot;
}

export interface GetGatewayConsolidatorRequest {
  gatewayId: ID;
}

export interface GetGatewayConsolidatorResponse {
  stats: ConsolidatorStats;
}
