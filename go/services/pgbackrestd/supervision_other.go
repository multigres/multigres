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

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
)

// Supported portable program contract: pgBackRest 2.60, generated local/TLS
// configuration, archive-async disabled. This is not a sandbox for arbitrary
// programs or future versions with additional session-detaching paths.
// The supervisor is the session AND group leader, reserving its own PID/PGID
// until it exits. Children inherit this group, including after root reaping.
func supervisorSysProcAttr() *syscall.SysProcAttr      { return &syscall.SysProcAttr{Setsid: true} }
func supervisedChildSysProcAttr() *syscall.SysProcAttr { return nil }

func becomeSubreaper() error {
	if syscall.Getpgrp() != os.Getpid() {
		return errors.New("portable supervisor has no private group anchor")
	}
	// Catch (not Ignore) TERM: exec resets the handler for children, while group
	// TERM leaves the anchor alive. Never use signal.Stop before process exit.
	signal.Notify(make(chan os.Signal, 1), syscall.SIGTERM)
	return nil
}

func signalOwnedChildren(_ int, sig syscall.Signal) error {
	// Never use the reaped root's PID. Our own live session anchor cannot be
	// reused. KILL deliberately destroys it: no ACK, no later numeric signal,
	// and the durable marker continues to veto cleanup/restart.
	err := syscall.Kill(-os.Getpid(), sig)
	if sig == syscall.SIGKILL {
		return errors.New("portable escalation destroyed group anchor; quiescence unproven")
	}
	return err
}
func portableGroupGone(_ int) bool { return portableAnchorAlone() }
