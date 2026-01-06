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

export type PoolerType = "PRIMARY" | "REPLICA";

// MultiOrch from clustermetadata.proto
export interface MultiOrch {
  id?: ID;
  hostname?: string;
  port_map?: PortMap;
}

// Job types and status
export type JobType = "UNKNOWN" | "BACKUP" | "RESTORE";
export type JobStatus = "UNKNOWN" | "PENDING" | "RUNNING" | "COMPLETED" | "FAILED";
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
