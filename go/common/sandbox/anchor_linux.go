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
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// OpenAnchor opens root with O_PATH|O_NOFOLLOW. For AnchorMount it verifies
// root is a mount point.
//
// O_PATH|O_NOFOLLOW alone proves neither: a final symlink opens as the link
// itself, and nothing says the directory is a mount. The check is therefore
// statx on the fd, which is spelling-independent and sees bind mounts of a
// subdirectory (what a Kubernetes volume mount is). A mount point cannot be
// renamed or removed, which is what makes the anchor stable afterwards.
func OpenAnchor(root string, mode AnchorMode) (*os.File, error) {
	fd, err := unix.Open(root, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("sandbox: opening anchor %q: %w", root, err)
	}
	f := os.NewFile(uintptr(fd), root)

	var st unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH, unix.STATX_TYPE, &st); err != nil {
		f.Close()
		return nil, fmt.Errorf("sandbox: statx anchor %q: %w", root, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		f.Close()
		return nil, fmt.Errorf("sandbox: anchor %q is not a directory (mode %#o)", root, st.Mode&unix.S_IFMT)
	}
	if mode == AnchorMount {
		if err := rejectPseudoRoot(fd, root); err != nil {
			f.Close()
			return nil, err
		}
		if st.Attributes_mask&unix.STATX_ATTR_MOUNT_ROOT == 0 {
			f.Close()
			return nil, fmt.Errorf("%w: kernel cannot report mount roots (statx, need 5.8+)", ErrUnsupported)
		}
		if st.Attributes&unix.STATX_ATTR_MOUNT_ROOT == 0 {
			f.Close()
			return nil, fmt.Errorf("sandbox: anchor %q is not a mount point", root)
		}
	}
	return f, nil
}

// rejectPseudoRoot refuses mount roots that are never a data volume. Granting
// "/" would hand the child the whole filesystem. Comparison is by device and
// inode, so no spelling of the path gets around it.
func rejectPseudoRoot(fd int, root string) error {
	var got unix.Stat_t
	if err := unix.Fstat(fd, &got); err != nil {
		return fmt.Errorf("sandbox: fstat anchor %q: %w", root, err)
	}
	for _, p := range []string{"/", "/proc", "/sys"} {
		var st unix.Stat_t
		if err := unix.Stat(p, &st); err != nil {
			continue
		}
		if st.Dev == got.Dev && st.Ino == got.Ino {
			return fmt.Errorf("sandbox: anchor %q is %s, which is never a data volume", root, p)
		}
	}
	return nil
}
