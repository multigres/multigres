// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sandbox confines a process to an explicit set of filesystem paths
// with Landlock.
//
// A policy is applied by a re-executed copy of the calling binary
// ("<self> sandbox-exec --policy <base64 JSON> -- argv..."), so the parent
// keeps its own, wider access and only the child that touches untrusted
// data is confined. Landlock restrictions are irreversible and inherited by
// every descendant, which is what makes the child safe to hand to tools that
// fork helpers of their own.
//
// Every filesystem right of the kernel's ABI is handled, up to the newest
// ABI go-landlock knows (V11); anything a rule does not grant is denied. On a
// kernel newer than that, rights it added are not handled and so remain
// allowed until go-landlock is upgraded. The wrapper takes the highest ABI
// the kernel offers and fails closed below an explicit floor; it never falls
// back to silent best-effort, because an unconfined sandbox is worse than
// none. Rules that grant Read or Write also grant connect(2) to pathname unix
// sockets where the ABI governs that (9 and later).
//
// # Wiring the sandbox-exec subcommand
//
// The host binary must route os.Args[2:] to [ExecMain] before any flag
// parsing or process setup (with cobra, DisableFlagParsing on the command, and
// ahead of servenv): nothing such as an exporter, log file or listener may be
// created in the process that is about to be confined. [ExecMain] always
// confines; a caller that decided not to confine must not wrap at all.
//
// # Whether to confine
//
// Landlock cannot be guaranteed on every host, so a deployment chooses a
// [Mode]: required (fail closed, the default), auto (confine when the host
// supports Landlock, run unconfined when it reports it unsupported) or off.
// [Decide] settles that once, at start, from a [Probe] of the kernel; the
// callers never re-decide per child. Only "unsupported" (ENOSYS, EOPNOTSUPP,
// an ABI below the floor, a non-Linux host) lets auto degrade. Any other probe
// failure, EPERM from a seccomp profile above all, fails the start in every
// mode, because degrading on it would turn a misconfigured profile into
// silently unconfined children.
//
// # What is not confined
//
// Landlock here does not scope signals or abstract unix sockets, at any ABI,
// and on ABI 7 and 8 (what the dev and staging nodes run) connect(2) to a
// pathname socket is not governed either. A confined child can therefore
// signal same-uid processes and reach sockets such as the control socket by
// path. Scoping is not used because a signal scope would break
// "pgbackrest stop --force", which must signal a detached async worker. The
// residual impact stays within the project: the design's threat model
// already accepts that a compromised postgres affects its own project's
// sidecar, and no cross-project credential is reachable this way.
//
// # Caller obligations
//
// Landlock does not police a file descriptor that is already open, so never
// pass a child an fd to something secret.
//
// Write includes creating symlinks, FIFOs and sockets. Any sidecar-private
// directory granted to a confined child (lock/, spool/, tmp/) can therefore
// have such objects planted in it, so the unsandboxed parent's own operations
// there must go through safefs (no-follow, beneath) and never open a FIFO
// blocking.
//
// Outside Linux [Probe] reports Landlock unsupported and every other call
// returns [ErrUnsupported].
package sandbox
