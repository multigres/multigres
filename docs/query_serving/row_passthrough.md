# Opaque Row Passthrough

## Overview

For a query whose rows the gateway only relays, decoding each PostgreSQL
`DataRow` in the multipooler, marshalling it into protobuf, and re-encoding it
in the multigateway is wasted work. With opaque row passthrough the multipooler
keeps the `DataRow` frames as the raw bytes PostgreSQL sent, ships them as one
byte block per result batch, and the multigateway writes that block to the
client socket unchanged.

Only `DataRow` frames travel this way. `RowDescription` (`fields`), the command
tag, notices, and `ParameterStatus` stay structured.

## How it is requested

Passthrough is opt-in per query through `ExecuteOptions.passthrough_row`
(`proto/query.proto`). The field defaults to false, so callers that read
column values themselves (multiadmin, multiorch, internal pooler queries) stay
on the structured path.

On the gateway, passthrough is the default for client queries. `ScatterConn`
sets `passthrough_row` unless the calling primitive passes `keepStructured`
(`wantPassthroughRow` in `scatterconn/scatter_conn.go`). A primitive opts out
when the gateway itself consumes the rows:

- `Route.KeepStructured`, set by the planner for the resolve step of the
  dynamic `set_config` shape (`planResolveSetConfig`, consumed by
  `ResolveTrackSetConfig`).
- The `SET` validation probe in `engine/validate_setting.go`, which reads a
  single value back.

## Wire representation

`QueryResult` carries three passthrough fields:

| Field                         | Meaning                                                                                               |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `passthrough_block`           | The next ordered chunk of raw `DataRow` wire bytes. `rows` is empty when it is set.                   |
| `passthrough_row_count`       | Number of `DataRow` frames completed in this block. Zero for an intermediate fragment of a large row. |
| `passthrough_row_in_progress` | The block ends inside a `DataRow` frame.                                                              |

Concatenating the blocks of a result in stream order reproduces the exact
`DataRow` byte stream of the backend. `sqltypes.Result` mirrors the fields as
`PassthroughBlock`, `PassthroughRowCount` and `PassthroughRowInProgress`.

## Multipooler side

The multipooler executor copies the option onto the backend connection for the
duration of the query (`SetPassthroughRow`) and clears it afterwards, so a
pooled connection never carries the mode into the next query.

In `pgprotocol/client`, `resultBatcher` builds the blocks:

- A row whose frame fits within `DefaultStreamingBatchSize` (2 MB) is read
  whole. Rows accumulate in one block, which is flushed when the next row would
  cross the batch size.
- A larger row gets blocks of its own. The frame header is written first, then
  the body is read from the socket in chunks of at most the batch size and each
  chunk is flushed as a block with `passthrough_row_in_progress` set. The row is
  counted on the block that holds its last byte.

A large row is therefore streamed without ever being buffered whole in the
multipooler.

## Multigateway side

`pgprotocol/server` writes a `PassthroughBlock` to the client with
`WriteRawMessage`, with no re-framing, and sends `RowDescription` from the
structured `Fields` as usual.

If the upstream stream fails while a row is in progress, the client has
received part of a `DataRow` frame and no further pgwire message would be
parseable. The server returns `errIncompleteDataRow`; the serve loop discards
its buffered output (`abortWriterBuffering`) and closes the connection instead
of appending an `ErrorResponse`.

## Reading rows from a result

Code that needs column values must call `Result.StructuredRows()`, not read
`Rows` directly. It panics when the result carries a passthrough block, which
turns a silently empty row set into an immediate failure if passthrough is ever
enabled on a path that interprets rows. `Result.RowCount()` returns the right
count in both modes.

## Code organization

| File                                         | Role                                                   |
| -------------------------------------------- | ------------------------------------------------------ |
| `proto/query.proto`                          | `ExecuteOptions.passthrough_row`, `QueryResult` fields |
| `go/common/sqltypes/sqltypes.go`             | `Result` fields, `RowCount`, `StructuredRows`          |
| `go/common/pgprotocol/client/query.go`       | `resultBatcher`, `streamPassthroughDataRow`            |
| `go/common/pgprotocol/server/packet.go`      | `WriteRawMessage`                                      |
| `go/common/pgprotocol/server/query.go`       | `errIncompleteDataRow`                                 |
| `go/services/multigateway/scatterconn/`      | `wantPassthroughRow`                                   |
| `go/services/multigateway/engine/route.go`   | `Route.KeepStructured`                                 |
| `go/services/multipooler/internal/executor/` | Sets and clears the mode around each query             |
