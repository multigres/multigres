//go:build linux

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
	"bytes"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

func currentWorkerOwner() (workerOwner, error) {
	owner := workerOwner{Platform: "linux", Token: uuid.NewString(), PID: os.Getpid()}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return owner, err
	}
	owner.BootID = strings.TrimSpace(string(boot))
	bootID, err := uuid.Parse(owner.BootID)
	if err != nil || bootID == uuid.Nil {
		return owner, errors.New("invalid kernel boot identity")
	}
	owner.BootID = bootID.String()
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return owner, err
	}
	first := bytes.IndexByte(stat, ' ')
	last := bytes.LastIndexByte(stat, ')')
	if first < 1 || last < first {
		return owner, errors.New("malformed owner process stat")
	}
	pid, err := strconv.Atoi(string(stat[:first]))
	if err != nil || pid != owner.PID {
		return owner, errors.New("procfs does not describe the current PID namespace")
	}
	fields := strings.Fields(string(stat[last+1:]))
	if len(fields) < 20 {
		return owner, errors.New("missing owner process start time")
	}
	owner.StartTicks, err = strconv.ParseUint(fields[19], 10, 64)
	if err != nil || owner.StartTicks == 0 {
		return owner, errors.New("invalid owner process start time")
	}
	var namespace unix.Stat_t
	if err := unix.Stat("/proc/self/ns/pid", &namespace); err != nil {
		return owner, err
	}
	owner.NamespaceDevice = namespace.Dev
	owner.NamespaceInode = namespace.Ino
	if owner.NamespaceDevice == 0 || owner.NamespaceInode == 0 {
		return owner, errors.New("invalid kernel PID namespace identity")
	}
	owner.Hostname, err = os.Hostname()
	if err != nil {
		return owner, err
	}
	if owner.Hostname == "" {
		return owner, errors.New("empty runtime hostname")
	}
	return owner, nil
}

// priorRuntimeDomainEnded is intentionally NOT a generic namespace-inequality
// test. PID-1 operation is the supported container deployment contract:
//   - StateDir is private to one container slot, never copied/shared between
//     independent live containers or hosts;
//   - its runtime serializes restart AFTER complete teardown of the prior PID
//     namespace (the kernel kills the namespace's descendants when init dies).
//
// Within that contract, changed boot/namespace identity under the same hostname
// identifies a completed container-domain replacement. The instance flock must
// also be held by the caller, excluding any still-running old owner/supervisor.
// Arbitrary sibling namespaces or storage fencing alone do NOT supply this
// runtime guarantee. Host/shared-PID and local sidecars are not PID 1 and cannot
// use this recovery path. Same-namespace supervisor death always fails closed.
func priorRuntimeDomainEnded(old, current workerOwner) bool {
	if old.Platform != "linux" || current.Platform != "linux" || old.PID != 1 || current.PID != 1 {
		return false
	}
	if old.Hostname == "" || old.Hostname != current.Hostname {
		return false
	}
	if old.BootID == "" || current.BootID == "" || old.NamespaceDevice == 0 || current.NamespaceDevice == 0 || old.NamespaceInode == 0 || current.NamespaceInode == 0 || old.StartTicks == 0 || current.StartTicks == 0 {
		return false
	}
	// nsfs inode numbers can be recycled immediately after teardown. PID 1
	// cannot be replaced in a live PID namespace, so its kernel start identity
	// distinguishes a recycled namespace inode. A fresh random token does not.
	return old.BootID != current.BootID || old.NamespaceDevice != current.NamespaceDevice || old.NamespaceInode != current.NamespaceInode || old.StartTicks != current.StartTicks
}
