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

// LimitABIForTest makes this process enforce the ruleset of Landlock ABI n
// when that is lower than the kernel's, the way a production node on an older
// kernel differs from CI. It is for test binaries that answer the sandbox-exec
// subcommand (their TestMain), and it is process-wide and irreversible.
func LimitABIForTest(n int) {
	effectiveABI = func() int { return min(ABI(), n) }
}
