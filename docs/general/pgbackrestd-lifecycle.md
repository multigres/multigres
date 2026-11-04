# pgbackrestd lifecycle safety

The Unix **control** API reports `instance_id` (the same startup UUID as
`restore_incarnation`) and `lifecycle_protocol_version`. Version 1 denotes the
coordinated supervised-lifetime, instance-matched shutdown and atomic static
`credentials.json` bundle contract. Deploy the matching bundle reader and
provisioner together; advertising a version alone does not validate credentials.
The WAL socket and pgBackRest TLS port do not expose Shutdown.

`Shutdown` requires the nonempty, exact current `instance_id`. Acceptance means
that this instance has closed admission and requested its own graceful shutdown,
not that its workers have already stopped. The HTTP handler's response is drained
before the control server closes. Callers must still wait for process exit and
acquire the held cleanup guard before destructive cleanup. Wrong, missing or stale
identities cannot stop a replacement instance. A server not running its serve
loop refuses Shutdown. Failed worker settlement propagates through Run and Close
and retains the ownership lock and pending evidence.

## Portable support boundary

Linux uses a per-invocation subreaper, including for detached async archive
workers. macOS supports the inspected **pgBackRest 2.60** local/TLS command paths
with archive-async disabled for archive-push/get. This is a program-dependent
contract, not a general descendant sandbox. Re-audit fork, setsid and external
execution paths before supporting a different pgBackRest version or configuration.

On macOS the supervisor is the private session/group leader. Synchronous children
inherit its group. Reaping the foreground child does not release that group
identity: the supervisor retains it through marker sync and acknowledgement.
`kern.proc.pgrp` must report only the supervisor; enumeration failure or remaining
members (including zombies) cannot authorize cleanup. Cooperative group TERM
leaves the supervisor alive. If KILL escalation is needed it also kills the
anchor: no quiescence acknowledgement is possible and the pending marker remains.
Subsequent startup therefore fails closed rather than signalling a reusable group
number or guessing that all members exited. Other non-Linux platforms have no
verified enumeration implementation and cannot certify settlement.

## Output and failure policy

Raw TLS-server stdout/stderr are best effort. Two process-wide forwarders retain
bounded queues (eight 32-KiB chunks each) even if a sink blocks forever. Queue
overflow drops bytes and reports the count when that sink can accept logs again.
Neither forwarding nor reporting is joined by restore pause or shutdown; global
stdio is never closed. Parsed INFO/VERIFY output is not routed through these
queues and retains the loss-detection/backpressure contract.

A definite pipe or supervisor-start failure durably removes its never-spawned
marker. A live supervisor also retires a definite child-start failure or a
pre-spawn stop after receiving its complete specification. Unknown handoff,
supervisor death and unsettled workers do not use that exception.

## Coordinated rollout

There is no operator image-capability gate. Upgrade the sidecar, clients and local
provisioner as a coordinated deployment. New instance locks cannot retroactively
prove the absence of workers from legacy sidecars. Do not infer safe legacy
cleanup from PID absence, an available lock, idle Status or a fresh instance ID.

Container recovery requires a private StateDir for one runtime-serialized slot,
a direct-PID-1 sidecar, and complete teardown of the previous PID namespace before
replacement. Init wrappers, shared PID namespaces and bare-host supervisor death
do not establish that boundary and remain fail-closed. Preserve PGDATA, restore
sentinels, operation journals and pending worker evidence when settlement fails.
