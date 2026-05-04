// grpc-gateway serializes google.protobuf.Duration as a JSON string (e.g. "5s").
// This module overrides the generated Duration struct so TypeScript types match
// the actual wire format. Referenced via Makefile --ts_proto_opt=M mapping.
export type Duration = string;
