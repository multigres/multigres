//go:build !linux && !darwin

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
	"runtime"
)

// readProcessTable has no process table to read on this platform. Returning an
// error, not an empty table, makes awaitServerWorkers wait out its bound
// instead of concluding the server has no workers.
func readProcessTable() (processTable, error) {
	return processTable{}, errors.New("no process table reader for " + runtime.GOOS)
}
