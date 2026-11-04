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

package local

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func openRecoveredSidecar(pid int) (func(syscall.Signal) error, func() error, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, nil, err
	}
	return func(sig syscall.Signal) error { return unix.PidfdSendSignal(fd, sig, nil, 0) }, func() error { return unix.Close(fd) }, nil
}
