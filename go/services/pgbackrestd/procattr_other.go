//go:build !linux

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

package pgbackrestd

import "syscall"

// childSysProcAttr has no Pdeathsig off Linux (the local provisioner on a
// developer machine): a crashed sidecar there can leave a child running, and
// the group kill on a clean shutdown is the only protection.
func childSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// groupGone reports whether the group has no member. Off Linux there is no
// /proc to tell a zombie from a live process, so a member counts until reaped.
func groupGone(pgid int) bool {
	return syscall.Kill(-pgid, 0) == syscall.ESRCH
}

// descendantsAlive has no /proc to walk off Linux; the server-worker wait is a
// Linux, pod-only concern, so it never waits here.
func descendantsAlive(int) bool { return false }
