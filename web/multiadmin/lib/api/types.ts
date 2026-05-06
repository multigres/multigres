// Re-exports from generated proto types. Do not edit manually — regenerate with: make proto

// Multiadmin service API types: requests, responses, enums
export * from "./generated/multiadminservice_pb.js";

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
} from "./generated/clustermetadata_pb.js";
export {
  ID_ComponentType,
  PoolerType,
  PoolerServingStatus,
} from "./generated/clustermetadata_pb.js";

// Gateway diagnostics types
export type {
  QueryStatSnapshot,
  QueryRegistrySnapshot,
  ConsolidatorStats,
  ConsolidatorPreparedStatement,
} from "./generated/multigatewaymanagerdata_pb.js";

// Pooler status types (proxied through GetPoolerStatusResponse)
export type {
  Status as PoolerStatus,
  PrimaryStatus,
  StandbyReplicationStatus,
  SynchronousReplicationConfiguration,
  PrimaryConnInfo,
} from "./generated/multipoolermanagerdata_pb.js";
export {
  PostgresStatus,
  PostgresAction,
} from "./generated/multipoolermanagerdata_pb.js";

// Enriched pooler combining topology metadata with live status (not a proto type).
// Uses PlainMessage<MultiPooler> so plain objects from JSON parsing satisfy this type.
import type { PlainMessage } from "@bufbuild/protobuf";
import type { MultiPooler } from "./generated/clustermetadata_pb.js";
import type { Status } from "./generated/multipoolermanagerdata_pb.js";
export interface MultiPoolerWithStatus extends PlainMessage<MultiPooler> {
  status?: PlainMessage<Status>;
}
