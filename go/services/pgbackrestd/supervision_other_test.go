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
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == SupervisorSubcommand {
		if err := SupervisorMain(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1) //nolint:forbidigo // TestMain routes the supervisor process exit status
		}
		os.Exit(0) //nolint:forbidigo // TestMain must not fall through to the test runner
	}
	os.Exit(m.Run()) //nolint:forbidigo // TestMain owns the test process exit
}
