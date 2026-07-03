// Re-exports from generated proto types. Do not edit manually — regenerate with: make proto

// Multiadmin service API types: requests, responses, enums
export * from "./generated/multiadminservice_pb";

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
} from "./generated/clustermetadata_pb";
export {
  ID_ComponentType,
  PoolerType,
  PoolerServingStatus,
} from "./generated/clustermetadata_pb";

// Gateway diagnostics types
export type {
  QueryStatSnapshot,
  QueryRegistrySnapshot,
  ConsolidatorStats,
  ConsolidatorPreparedStatement,
} from "./generated/multigatewaymanagerdata_pb";

// Pooler status types (proxied through GetPoolerStatusResponse)
export type {
  Status as PoolerStatus,
  PrimaryStatus,
  StandbyReplicationStatus,
  SynchronousReplicationConfiguration,
  PrimaryConnInfo,
} from "./generated/multipoolermanagerdata_pb";
export {
  PostgresStatus,
  PostgresAction,
  SynchronousCommitLevel,
  SynchronousMethod,
} from "./generated/multipoolermanagerdata_pb";

// Enriched pooler combining topology metadata with live status (not a proto type).
// Uses PlainMessage<MultiPooler> so plain objects from JSON parsing satisfy this type.
import type { PlainMessage } from "@bufbuild/protobuf";
import type { MultiPooler } from "./generated/clustermetadata_pb";
import type { Status } from "./generated/multipoolermanagerdata_pb";
export interface MultiPoolerWithStatus extends PlainMessage<MultiPooler> {
  status?: PlainMessage<Status>;
}
