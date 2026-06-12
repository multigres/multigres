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

package pgutil

import "github.com/jackc/pglogrepl"

// LSN is a PostgreSQL Log Sequence Number.
type LSN = pglogrepl.LSN

// ParseLSN parses a PostgreSQL LSN string (e.g. "0/16E5D38") into an LSN.
func ParseLSN(s string) (LSN, error) {
	return pglogrepl.ParseLSN(s)
}
