// Re-exports from generated proto types. Do not edit manually — regenerate with: make proto

// Multiadmin service API types: requests, responses, enums
export * from "./generated/multiadminservice.generated";

// Cluster topology types
export type {
  Cell,
  Database,
  ID,
  MultiGateway,
  MultiPooler,
  MultiOrch,
  ConsensusStatus,
  AvailabilityStatus,
} from "./generated/clustermetadata.generated";
export {
  ID_ComponentType,
  PoolerType,
  PoolerServingStatus,
} from "./generated/clustermetadata.generated";

// Gateway diagnostics types
export type {
  QueryStatSnapshot,
  QueryRegistrySnapshot,
  ConsolidatorStats,
  ConsolidatorPreparedStatement,
} from "./generated/multigatewaymanagerdata.generated";

// Pooler status types (proxied through GetPoolerStatusResponse)
export type {
  Status as PoolerStatus,
  PrimaryStatus,
  StandbyReplicationStatus,
  SynchronousReplicationConfiguration,
  PrimaryConnInfo,
} from "./generated/multipoolermanagerdata.generated";
export {
  PostgresStatus,
  PostgresAction,
} from "./generated/multipoolermanagerdata.generated";

// Enriched pooler combining topology metadata with live status (not a proto type)
import type { MultiPooler } from "./generated/clustermetadata.generated";
import type { Status } from "./generated/multipoolermanagerdata.generated";
export interface MultiPoolerWithStatus extends MultiPooler {
  status?: Status;
}
