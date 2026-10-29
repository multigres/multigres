// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package safefs

import (
	"errors"
	"strings"

	"golang.org/x/sys/unix"
)

// resolveFlags make the kernel refuse, during path resolution itself, every
// way out of the root: symlinks, magic links (/proc/self/fd/N) and ".."
// escapes. NO_XDEV keeps a mount nested under the root unreachable.
// Resolving in the kernel is what closes the check-then-open race a
// userspace walk would have.
const resolveFlags = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV

// openat2Retries bounds retries of EAGAIN, which RESOLVE_BENEATH returns when
// a concurrent rename makes the kernel unable to prove the path stayed beneath.
const openat2Retries = 10

func openRootFD(dir string) (int, error) {
	return unix.Open(dir, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
}

// openBeneath opens comps beneath dirfd. There is no fallback when the kernel
// lacks openat2 (ENOSYS): a walk without kernel-side resolution would quietly
// weaken the guarantee, so the call fails instead.
func openBeneath(dirfd int, comps []string, flags int, mode uint32) (int, error) {
	path := "."
	if len(comps) > 0 {
		path = strings.Join(comps, "/")
	}
	how := &unix.OpenHow{Flags: uint64(flags), Mode: uint64(mode), Resolve: resolveFlags}
	for range openat2Retries {
		fd, err := unix.Openat2(dirfd, path, how)
		if errors.Is(err, unix.EAGAIN) {
			continue
		}
		return fd, err
	}
	return -1, unix.EAGAIN
}
