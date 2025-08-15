# multigres Protobuf Definitions

This directory contains all multigres protobuf definitions.

Our protobuf messages are both used as wire format (e.g. `query.proto`) and for
storage (e.g. `clustermetadata.proto`).

RPC messages and service definitions are in separate files (e.g. `mtgateway.proto`
and `mtgatewayservice.proto`) on purpose because our internal deployment does not
use gRPC.

## Style Guide

Before creating new messages or services, please make yourself familiar with the
style of the existing definitions first.

Additionally, new definitions must adhere to the Google Cloud API Design Guide:
https://cloud.google.com/apis/design/