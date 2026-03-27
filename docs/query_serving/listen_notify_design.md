# LISTEN/NOTIFY

## Overview

Multigres supports PostgreSQL's asynchronous notification system
(`LISTEN`, `UNLISTEN`, `NOTIFY`) using a shared listener architecture.
Instead of pinning a backend connection per client (which defeats
connection pooling), a single dedicated PostgreSQL connection per
multipooler instance listens on behalf of all gateway clients.
Notifications are fanned out through gRPC streaming to each gateway,
which delivers them to the appropriate client sessions.

Key capabilities:

- `LISTEN channel` / `UNLISTEN channel` / `UNLISTEN *` handled in
  the gateway with proper transaction semantics
- `NOTIFY channel [, payload]` routed to PostgreSQL as a regular query
- `pg_notify()` function works transparently (it's just a regular query)
- Per-channel refcounting: PostgreSQL `LISTEN`/`UNLISTEN` issued only
  on first subscriber / last unsubscriber
- Automatic reconnect on dedicated listener connection failure
- Async delivery to idle clients between queries

## Background

PostgreSQL's `LISTEN`/`NOTIFY` provides a simple pub/sub mechanism.
A session issues `LISTEN channel` to subscribe, and any session can
issue `NOTIFY channel, 'payload'` to broadcast a message.
Notifications are delivered asynchronously — they arrive as
`NotificationResponse` ('A') messages on the wire, typically between
query results.

In a connection pooler, this creates a tension: `LISTEN` normally
pins the session to a specific backend connection (because
notifications are delivered on the connection that issued `LISTEN`).
This breaks pooling. The shared listener approach solves this by
decoupling notification delivery from query execution.

## Architecture

Both the simple query protocol (`HandleQuery`) and extended query protocol
(`HandleExecute` via Parse/Bind/Execute) are supported. `PlanPortal`
intercepts LISTEN/UNLISTEN/NOTIFY in the extended path so they receive
the same local handling as the simple path.

```text
Client
  │
  ▼
MultiGateway Handler (handler.go)
  │  - Parses LISTEN/UNLISTEN/NOTIFY
  │  - Delivers notifications to client socket
  │  - Supports simple and extended query protocols
  │
  ├─── [NOTIFY] ──► Planner → Route → PostgreSQL (regular query)
  │
  └─── [LISTEN/UNLISTEN] ──► Planner / PlanPortal → ListenNotifyPrimitive
                                │
                                ▼
                    Engine (listen_notify.go, transaction_primitive.go)
                      │  - Updates connection state
                      │  - Autocommit: syncs subscriptions via SubSync
                      │  - In transaction: buffers as pending
                      │  - COMMIT: syncs pending subscriptions via SubSync
                      │  - ROLLBACK: discards pending subscriptions
                      │
                      ▼
                    SubscriptionSync → NotificationManager (GRPCNotificationManager)
                      │  - Per-channel gRPC streams to multipooler
                      │  - Automatic reconnect on stream failure
                      │  - Local refcounting and fan-out
                      │
                      ▼
                    Multipooler gRPC Service
                      │  - StreamNotifications RPC
                      │
                      ▼
                    PubSubListener (listener.go)
                      │  - Single dedicated PG connection
                      │  - Channel refcounting
                      │  - Fan-out to all gRPC subscribers
                      │
                      ▼
                    PostgreSQL (LISTEN/UNLISTEN on dedicated conn)
```

## Notification Delivery Flow

When a notification is emitted (via `NOTIFY` or `pg_notify()`):

```text
PostgreSQL
  │  sends NotificationResponse on dedicated listener conn
  ▼
PubSubListener (multipooler)
  │  reader goroutine reads all PG messages via ReadRawMessage()
  │  fans out notifications to all subscribers for that channel
  ▼
gRPC StreamNotifications (multipooler → gateway)
  │  streams PgNotification proto
  ▼
GRPCNotificationManager (gateway)
  │  fans out to local client NotifCh channels
  ▼
forwardNotifications goroutine
  │  reads NotifCh → writes to AsyncNotifCh
  ▼
Async pusher goroutine (server.Conn)
  │  reads AsyncNotifCh → writes 'A' message to client socket
  ▼
Client receives NotificationResponse
```

Between queries, the async pusher writes directly to the client
socket. After a query completes (before `ReadyForQuery`),
`flushNotifications()` drains any pending notifications and writes
them synchronously.

## Transaction Semantics

`LISTEN` and `UNLISTEN` inside transactions are deferred until
`COMMIT`, matching PostgreSQL behavior.

```sql
BEGIN;
LISTEN mychannel;    -- buffered as pending
LISTEN other;        -- buffered as pending
ROLLBACK;            -- pending listens discarded, no subscription
```

```sql
BEGIN;
LISTEN mychannel;    -- buffered as pending
COMMIT;              -- subscription now active
```

Outside a transaction (autocommit), `LISTEN`/`UNLISTEN` take effect
immediately.

Multi-statement batches (`LISTEN ch; SELECT 1;`) are wrapped in an
implicit transaction. The pending listens are committed when the
implicit transaction commits at the end of the batch.

### Connection State

Per-connection state tracked in `MultiGatewayConnectionState`:

| Field                | Type                 | Purpose                                  |
| -------------------- | -------------------- | ---------------------------------------- |
| `ListenChannels`     | `map[string]bool`    | Active subscriptions                     |
| `PendingListens`     | `[]string`           | Channels to subscribe on COMMIT          |
| `PendingUnlistens`   | `[]string`           | Channels to unsubscribe on COMMIT        |
| `PendingUnlistenAll` | `bool`               | UNLISTEN \* pending on COMMIT            |
| `NotifCh`            | `chan *Notification` | Receives notifications from NotifManager |
| `AsyncNotifCh`       | `chan<- *...`        | Sends to async pusher goroutine          |
| `SubSync`            | `SubscriptionSync`   | Syncs subscriptions from engine primitives|

## PubSubListener

The `PubSubListener` in the multipooler manages a single dedicated
PostgreSQL connection for all `LISTEN`/`NOTIFY` operations.

- **Serialized event loop**: All subscribe/unsubscribe requests are
  serialized through a buffered channel, avoiding the need for locks
  on internal state.
- **Channel refcounting**: Tracks the number of subscribers per
  channel. `LISTEN` is sent to PostgreSQL only when the first
  subscriber arrives; `UNLISTEN` when the last one leaves.
- **Split read/write**: TCP is full-duplex; `bufferedReader` and
  `bufferedWriter` are independent bufio instances. The reader
  goroutine exclusively reads all PG messages via `ReadRawMessage()`
  (notifications, command completions, errors). The event loop
  exclusively writes `LISTEN`/`UNLISTEN` commands via `SendQuery()`.
  This allows issuing commands on the live connection without
  reconnecting, preventing notification loss for existing channels.
- **Reconnect**: On reader error (connection failure), the connection
  is closed and reconnected after a delay (2s initial, 5s on repeated
  failure). All active channels are re-LISTENed on the new connection.
  Reconnect only happens on actual connection failure, not on
  subscribe/unsubscribe operations.

## Gateway-Side Subscription Management

`GRPCNotificationManager` manages per-channel gRPC streams from the
gateway to the multipooler:

- First local subscriber for a channel opens a
  `StreamNotifications` gRPC stream to the multipooler.
- Additional subscribers for the same channel share the stream; the
  manager fans out locally.
- When the last subscriber for a channel unsubscribes, the gRPC
  stream is cancelled.
- On client disconnect, `UnsubscribeAll` removes all subscriptions
  and cancels orphaned streams.

## Cleanup

When a client disconnects:

1. `handler.ConnectionClosed()` calls `notifMgr.UnsubscribeAll()`
2. GRPCNotificationManager removes the client from all channels,
   cancelling empty gRPC streams
3. Multipooler-side gRPC handler unsubscribes from PubSubListener
4. PubSubListener refcount drops; if zero, `UNLISTEN` sent to PG
5. Async pusher goroutine stopped, channels cleared

## Resilience

- **gRPC stream reconnect.** If the gRPC stream between the gateway's
  GRPCNotificationManager and the multipooler breaks (e.g., multipooler
  restart), the stream is automatically re-established with a 2-second
  backoff. Notifications sent during the reconnect window are lost.
- **PubSubListener reconnect.** If the dedicated PostgreSQL connection
  drops, PubSubListener reconnects with backoff (2s initial, 5s on
  repeated failure) and re-LISTENs all active channels.

## Known Limitations

- **Notifications may be lost during reconnect.** Both the PG listener
  connection and the gRPC streams reconnect automatically, but any
  notifications sent during either reconnect window are lost.
  Subscribers are not notified of the gap. Applications that require
  guaranteed delivery should use a more robust mechanism (e.g., polling
  a table or using a message queue).

## Future Optimizations

- **Single gRPC stream per gateway-pooler pair.**
  `GRPCNotificationManager` currently opens one `StreamNotifications`
  gRPC stream per unique PG channel. If a gateway has clients listening
  on many distinct channels, this creates many streams. Since HTTP/2
  multiplexes all streams over a single TCP connection, the overhead is
  small in practice. However, a single bidirectional stream with dynamic
  channel add/remove would reduce per-channel overhead and simplify
  stream lifecycle management. The `StreamNotifications` RPC already
  accepts `repeated string channels` — the multipooler side supports
  multi-channel subscriptions on a single stream.
