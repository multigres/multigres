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

//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/landlock-lsm/go-landlock/landlock"
	ll "github.com/landlock-lsm/go-landlock/landlock/syscall"
)

// resolveUnixABI is the first ABI that governs connect(2) on pathname unix
// sockets. From there on a rule must grant it explicitly or pgBackRest cannot
// reach the postgres socket or creds.sock.
const resolveUnixABI = 9

// configs[i] handles exactly the rights of ABI i+1. Selecting the entry for
// the kernel's own ABI gives the widest handled set without go-landlock's
// BestEffort downgrade, which would silently shrink the set.
var configs = []landlock.Config{
	landlock.V1, landlock.V2, landlock.V3, landlock.V4, landlock.V5, landlock.V6,
	landlock.V7, landlock.V8, landlock.V9, landlock.V10, landlock.V11,
}

// ABI returns the kernel's Landlock ABI version, or 0.
func ABI() int {
	v, err := ll.LandlockGetABIVersion()
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// Probe asks the kernel what Landlock it offers.
func Probe() (Capability, error) { return ProbeWith(ll.LandlockGetABIVersion) }

// CheckAvailable returns nil when Landlock works at ABI >= the floor, an error
// wrapping ErrUnsupported when it does not, and the probe's own error when the
// kernel's answer was neither.
func CheckAvailable() error {
	c, err := Probe()
	if err != nil {
		return err
	}
	if !c.Supported {
		return fmt.Errorf("%w: %s", ErrUnsupported, c.Reason)
	}
	return nil
}

const (
	accessRead = ll.AccessFSReadFile | ll.AccessFSReadDir
	accessExec = ll.AccessFSExecute
	// Write deliberately omits Refer and the device node rights: renames
	// within one directory (temp file plus rename) need only create and
	// remove.
	accessWrite = ll.AccessFSWriteFile | ll.AccessFSTruncate |
		ll.AccessFSMakeReg | ll.AccessFSMakeDir | ll.AccessFSMakeSym |
		ll.AccessFSMakeSock | ll.AccessFSMakeFifo |
		ll.AccessFSRemoveFile | ll.AccessFSRemoveDir
	// Rights that the kernel rejects on a non-directory.
	accessFileOnly = ll.AccessFSExecute | ll.AccessFSWriteFile | ll.AccessFSReadFile | ll.AccessFSTruncate
)

// landlockRule resolves r.Path afresh, by path, rather than through the fd
// OpenAnchor validated. The anchor check and this rule are therefore two
// resolutions of the same path, and they agree only because nothing that can
// run concurrently is able to change what the path resolves to: every
// component above the data volume's mount point is container root filesystem,
// which the other containers in the pod (postgres included) cannot write, and
// the mount point itself cannot be replaced by an unprivileged process. If a
// layout ever put the data volume below a directory another container can
// write, the anchor would have to be passed to the rule (an fd-based
// PathBeneath) instead, or the check and the rule could name different
// directories.
func (r Rule) landlockRule(abi int) (landlock.Rule, error) {
	info, err := os.Stat(r.Path)
	if err != nil {
		return nil, fmt.Errorf("sandbox: rule path: %w", err)
	}
	var set uint64
	if r.Access&Read != 0 {
		set |= accessRead
	}
	if r.Access&Write != 0 {
		set |= accessWrite
	}
	if r.Access&Exec != 0 {
		set |= accessExec
	}
	if !info.IsDir() {
		set &= accessFileOnly
	}
	if abi >= resolveUnixABI && r.Access&(Read|Write) != 0 {
		set |= ll.AccessFSResolveUnix
	}
	return landlock.PathAccess(landlock.AccessFSSet(set), r.Path), nil
}

// RestrictSelf applies policy to the calling process and every future child.
// A rule whose path does not exist is an error: skipping it would leave the
// operator believing something is granted that is not.
func RestrictSelf(policy Policy) error {
	if err := CheckAvailable(); err != nil {
		return err
	}
	return restrictAt(policy, effectiveABI())
}

// effectiveABI is a seam so tests can enforce an older ABI's ruleset on a
// newer kernel, which is how production (ABI 7 and 8 nodes) differs from CI.
var effectiveABI = ABI

func restrictAt(policy Policy, abi int) error {
	if abi < minABI {
		return fmt.Errorf("%w: ABI %d, need >= %d", ErrUnsupported, abi, minABI)
	}
	rules := make([]landlock.Rule, 0, len(policy.Rules))
	for _, r := range policy.Rules {
		lr, err := r.landlockRule(abi)
		if err != nil {
			return err
		}
		rules = append(rules, lr)
	}
	cfg := configs[min(abi, len(configs))-1]
	if err := cfg.RestrictPaths(rules...); err != nil {
		if errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("sandbox: applying Landlock ABI %d ruleset: %w (a seccomp profile may be blocking landlock_add_rule or landlock_restrict_self)", abi, err)
		}
		return fmt.Errorf("sandbox: applying Landlock ABI %d ruleset: %w", abi, err)
	}
	return nil
}
