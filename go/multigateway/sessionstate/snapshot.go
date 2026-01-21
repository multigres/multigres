// Copyright 2025 Supabase, Inc.
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

package sessionstate

// StateSnapshot captures session state before a SET/RESET command
// for potential rollback if the command fails.
type StateSnapshot struct {
	Kind SetCommandKind

	// For SET and RESET of individual variables
	Variable    string
	PrevValue   string
	PrevExisted bool

	// For RESET ALL
	AllSettings map[string]string
}
