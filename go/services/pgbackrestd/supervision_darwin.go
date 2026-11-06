//go:build darwin

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

import (
	"os"

	"golang.org/x/sys/unix"
)

// kern.proc.pgrp includes zombies and descendants reparented to launchd.
// Only our own entry permits ACK; errors/hidden or missing anchor fail closed.
// Once alone no remaining member can fork: the live private session leader
// reserves the group identity through marker sync and ACK.
func portableAnchorAlone() bool {
	pid := os.Getpid()
	members, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pid)
	return err == nil && len(members) == 1 && int(members[0].Proc.P_pid) == pid
}
