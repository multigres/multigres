//go:build !linux

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
	"golang.org/x/sys/unix"
)

// There is no openat2 outside Linux, so each component is opened with
// O_NOFOLLOW relative to the previous descriptor. That refuses a symlink in
// every component, but it is a userspace walk and not atomic with respect to
// the kernel's own resolution. It is enough for local development on macOS;
// the guarantee production relies on is the Linux implementation.

// There is no O_PATH on darwin, so a plain read-only directory fd stands in.
const rootFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

func openRootFD(dir string) (int, error) {
	return unix.Open(dir, rootFlags, 0)
}

func openBeneath(dirfd int, comps []string, flags int, mode uint32) (int, error) {
	cur, err := unix.Dup(dirfd)
	if err != nil {
		return -1, err
	}
	if len(comps) == 0 {
		_ = unix.Close(cur)
		return unix.Openat(dirfd, ".", flags|unix.O_NOFOLLOW, mode)
	}
	for i, c := range comps {
		f := flags | unix.O_NOFOLLOW
		if i < len(comps)-1 {
			f = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		}
		next, err := unix.Openat(cur, c, f, mode)
		_ = unix.Close(cur)
		if err != nil {
			return -1, err
		}
		cur = next
	}
	return cur, nil
}
