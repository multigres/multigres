# OpenAPI annotations

`annotations.proto` and `OpenAPIv3.proto` are vendored from
[google/gnostic v0.7.1](https://github.com/google/gnostic/tree/v0.7.1/openapiv3).
Only trailing blank lines are removed. Their Apache 2.0 license headers are preserved. The matching Go module is
pinned in `go.mod`. These files supply the `openapi.v3` options used by
`multiadminservice.proto` and understood by `protoc-gen-connect-openapi`.
