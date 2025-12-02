//go:build !windows

/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications Copyright 2025 Supabase, Inc.
*/

package servenv

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func (sv *ServEnv) pprofInit() error {
	prof, err := sv.parseProfileFlag(sv.pprofFlag.Get())
	if err != nil {
		return fmt.Errorf("parsing pprof flags: %w", err)
	}
	if prof != nil {
		start, stop := prof.init()

		// Start profiling immediately if waitSig is false
		if !prof.waitSig {
			start()
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGUSR1)

		go func() {
			for range sigChan {
				// Check current state and toggle
				if isProfileStarted() {
					stop()
				} else {
					start()
				}
			}
		}()

		sv.OnTerm(stop)
	}
	return nil
}
